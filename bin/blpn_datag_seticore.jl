using Dates
start = now()

using BLDirWalkers
using BLDirWalkers: unixms, qstatus, workerhostpid
using BLDirWalkers.Seticore: AbstractCapnpInfo, HitInfo, StampInfo
using CSV
using Distributed
using Sockets
using DuckDB
using DuckDB: DBInterface as DBI
using StructArrays
import Profile

#---
# Refuse to run if running with fewer than 7 threads (1 per queue * 4 queues +
# 1 for dwtask + for dbtask + minimum 1 for in-process directory agent).
nthreads = Threads.nthreads()
nthreads < 7 && error("refusing to run with fewer than 7 threads (nthreads=$nthreads)")

#---
# Start workers
#
# TODO Bring this long comment up to date!
#
# For BLUSE at MeerKAT, the hits and stamps files are archived under
# `/datag<N>/<hostname>/data/`.  On each compute/processing node this is
# symlnked as `/datag/<hostname>/data`.  Using a `topdirs` directory list that
# includes `/datag/<hostname>/data` for all hostnames is recommended.  Any
# directories that don't exist will be ignored, but existing directories will
# not be missed.  Not sure what the optimum number of diragents is (need to try
# it to find out), but running one in-process diragent is probably a good start.
# SeticoreCapnp is not single threaded (unlike HDF5), but DirWalkers only
# uses a single thread in fileagents, so multiple fileagent processes on
# multiple hosts is probably best.
#
# Due to the remote nature of `/datag` and the imbalance in per-host data
# volumes it is probably best not to limit workers to their own
# /datag/<hostname> directory.
#
# Due to the "silo" nature of the gluster volumes at MeerKAT, each silo (i.e.
# pair of racks) needs its own `dirq` and `fileq`.  To ensure the data are all
# coaleced, a single `outq` is desired.
#
# This script starts multiple remote worker processes per host on a
# silo-by-silo basis.  The first worker of each silo will be a diragent, the
# rest will be file agents.  This script can be run on any host, but the
# database is written to `/datag/users/seticoredb` (which must exist!) so it's
# best to run on a processing node in the "silo" corresponding to the storage
# node on which the database should be created.

ENV["JULIA_WORKER_TIMEOUT"] = 120.0

function exeflags(; project=dirname(@__DIR__), threads=4)
    (;
        exeflags=`--project="$project" --threads=$threads`
    )
end

function start_workers(workerspec; kwargs...)
    addprocs(workerspec; exeflags()..., kwargs...)
end

# At the time of this writing, blpn24 was inaccessible
silohosts = [
    ["blpn$i" for i in 0:15],      # /datag0 "silo"
    ["blpn$i" for i in 16:31 if i != 24], # /datag1
    ["blpn$i" for i in 32:47],     # /datag2 "silo"
    ["blpn$i" for i in 48:63],     # /datag3 "silo"
    ["blpn$i" for i in 64:2:78],   # /datag4 "silo"
    ["blpn$i" for i in 80:2:94],   # /datag5 "silo"
    ["blpn$i" for i in 96:2:110],  # /datag6 "silo"
    ["blpn$i" for i in 112:2:126], # /datag7 "silo"
]

workersperhost = 4 # :auto
silospecs = [[(h, workersperhost) for h in silo] for silo in silohosts]

# TODO Make testing work at the data center
#=
silospecs = [ # For TESTing at Berkeley data center
    [("blpc3", 2)],
    [("blpc3", 2)]
]
=#

@info "starting workers"
oversubcribe = 1

@time begin
    silows = map(silospecs) do hostspecs
        reduce(vcat, start_workers(hostspecs) for _ in 1:oversubcribe)
    end

    # Start queue worker processes on remote hosts
    # qws is dimensioned [4, nsilos]
    # topqs=qws[1,:] run on worker on first host of each silo
    # dirqs=qws[2,:] run on worker on second host of each silo
    # fileqs=qws[3,:] run on worker on third host of each silo
    # outqs=qws[4,:] run on worker on fourth host of each silo
    qws = mapreduce(hcat, silohosts) do silo
        start_workers(silo[2:5]; exeflags(threads=:auto)...)
    end
end

@info "$(sum(length, silows))+$(length(qws)) workers started"

#---
# Initialize workers

@info "initalize workers"
@time @everywhere begin
    using Dates
    using BLDirWalkers
    using BLDirWalkers: unixms, qstatus, workerhostpid
    using BLDirWalkers.Seticore: AbstractCapnpInfo, HitInfo, StampInfo
    using CSV
    using DuckDB
    using DuckDB: DBInterface as DBI
    using StructArrays
    import Profile

    # Print profiling data to "$(unixms()).profile"
    Profile.peek_report[] = ()->Profile.print(joinpath(@__DIR__, "$(unixms()).profile"); groupby=:task)

    function writeheader(io, ::Type{T}) where T<:AbstractCapnpInfo
        println(io, join(fieldnames(T), ","))
    end

    """
        output_handler(outdir, outq, progress_channel)

    Takes HitInfo/StampInfo objects from outq and writes them to
    "hits.HOSTNAME.csv" and "stamps.HOSTNAME.csv" in outdir.  Periodically send
    progress update to `progress_channel`.
    """
    function output_handler(outdir, outq, progress_channel)
        hostname = gethostname()
        iohits = open(joinpath(outdir, "hits.$hostname.csv"), "w")
        iostamps = open(joinpath(outdir, "stamps.$hostname.csv"), "w")

        put!(progress_channel, (; hostname, msg="writing to output files"))
        hitcount = 0
        stampcount = 0
        try
            writeheader(iohits, HitInfo)
            writeheader(iostamps, StampInfo)

            for items in Iterators.takewhile(!isnothing, outq)
                io = if typeof(items[1]) == HitInfo
                    # Increment hitcount
                    hitcount += length(items)
                    iohits
                else
                    # Increment stampcount
                    stampcount += length(items)
                    iostamps
                end

                CSV.write(io, items; append=true)

                if (hitcount + stampcount) % 100 == 0
                    put!(progress_channel, (; time=unixms(), hostname, hitcount, stampcount))
                end
            end
        finally
            close(iohits)
            close(iostamps)
        end

        put!(progress_channel, (; time=unixms(), hostname, hitcount, stampcount, final=true))
        put!(progress_channel, (; hostname, msg="done writing to output files"))

        hitcount, stampcount
    end
end
@info "workers initialized"

#---
# Set topdirs

#topdirs = ["/datag/mattl"]
topdirs = ["/datag/blpn$i" for i in Iterators.flatten((0:63, 64:2:126))]
#topdirs = ["/datax/scratch/jwst-test"] # For TESTing at Berkeley data center

outdir = "/datag/inventory"

#---
# Create DirWalker queues and progress_channel.

# Make RemoteQueues on queue workers of each silo
topqs = map(qw->RemoteTopQueue(qw; sz=Inf), qws[1,:])
dirqs = map(qw->RemoteDirQueue(qw; sz=Inf), qws[2,:])
fileqs = map(qw->RemoteFileQueue(qw; sz=Inf), qws[3,:])
outqs = map(qw->RemoteOutQueue{Vector}(qw; sz=Inf), qws[4,:])

# Output handlers will post progress updates here
progress_channel = RemoteChannel(()->Channel{NamedTuple}(Inf))

#---
# Start DirWalkers running on topq workers

# Log worker hosts/PIDs
open(joinpath(@__DIR__, "workers.txt"), "w") do io
    println(io, "topqs on ", join(workerhostpid.(qws[1,:])), " ")
    println(io, "dirqs on ", join(workerhostpid.(qws[2,:])), " ")
    println(io, "fileqs on ", join(workerhostpid.(qws[3,:])), " ")
    println(io, "outqs on ", join(workerhostpid.(qws[4,:])), " ")
end

# filefunc and filepred must be defined @everywhere!
dwfutures = map(silows, topqs, dirqs, fileqs, outqs) do ws, topq, dirq, fileq, outq
    @spawnat ws[1] run_dirwalker(
        Seticore.filefunc, topq, dirq, fileq, outq, topdirs;
        filepred=Seticore.filepred,
        dagentspec=ws[1:1],
        fagentspec=ws[2:end],
        extraspec=ws[1:1]
    )
end

#---
# Run output_handler on oqworkers

# Run output_handler on out queue worker
outputfutures = map(qws[4,:], outqs) do oqworker, outq
    @spawnat oqworker output_handler(outdir, outq, progress_channel)
end

#---
# Connect to database and create tables

#=
@info "create new database"
# connect to database (for main task)
#dbfile = "/datag/users/seticoredb/seticorefiles.duckdb"
dbfile = "/datax/scratch/davidm/filedb/seticorefiles.duckdb" # TESTing
rm(dbfile; force=true)
db = DBI.connect(DuckDB.DB, dbfile)

@info "creating table $hittab for HitInfo records"
@info "creating table $stamptab for StampInfo records"

DuckDB.register_data_frame(db, StructArray{Seticore.HitInfo}(undef, 0), "mockhit")
DuckDB.register_data_frame(db, StructArray{Seticore.StampInfo}(undef, 0), "mockstamp")

DBI.execute(db, "create table $hittab as select 1 as 'id', * from mockhit")
DBI.execute(db, "create table $stamptab as select 1 as 'id', * from mockstamp")

DuckDB.unregister_table(db, "mockhit")
DuckDB.unregister_table(db, "mockstamp")
=#

#---
# Define appender functions

#=
function appendrows(appender, id, itemvec)
    for item in itemvec
        id += 1
        DuckDB.append(appender, id)
        for i in 1:fieldcount(typeof(item))
            DuckDB.append(appender, getfield(item, i))
        end
        DuckDB.end_row(appender)
    end
    id
end

"""
For each AbstractCapnp item from `outq`, append a row to proper table in
database `db`.
"""
function run_appender(db, outq, npending=1; hittab="seticorehits", stamptab="seticorestamps")
    # Create Appenders append rows to database for each item in outq
    hitappender = DuckDB.Appender(db, hittab)
    stampappender = DuckDB.Appender(db, stamptab)

    @info "writing records to database"
    hitid = 0
    stampid = 0
    try
        for itemvec in outq
            if itemvec === nothing
                npending -= 1
                npending > 0 && continue
                break
            elseif itemvec isa Vector{Seticore.HitInfo}
                # Use hitid for id column
                hitid = appendrows(hitappender, hitid, itemvec)
            elseif itemvec isa Vector{Seticore.StampInfo}
                # Use stampid for id column
                stampid = appendrows(stampappender, stampid, itemvec)
            else
                @warn "ignoring unexpected type $(typeof(itemvec)) from output queue"
            end

            if (hitid + stampid) % 100_000 == 0
                @info "found $hitsid hits and $stampid stamps so far ($(now()-start))"
            end
        end
    finally
        DuckDB.close(hitappender)
        DuckDB.close(stampappender)
    end
    @info "done writing records to database"

    hitid, stampid
end
=#

#---
# Run dirwalker database appender

#=
npending = length(dwfutures)
hitcount, stampcount = run_appender(db, outq, npending)
=#

#---
# Start progress reporter task

progresstask = Threads.@spawn begin
    for nt in Iterators.takewhile(!isempty, progress_channel)
        println(nt)
    end
end |> errormonitor

#---
# Start qstatus monitor

qstatustask = Threads.@spawn begin
    t0 = unixms()
    ticks = 0
    while !istaskdone(progresstask)
        if ticks == 100
            for (silo, qs) in enumerate(zip(topqs, dirqs, fileqs, outqs))
                qstatus(; time=unixms()-t0, silo, qs...)
            end
            ticks = 0
        end

        sleep(0.1)
        tick += 1
    end
end |> errormonitor

#---
# Wait for completions

# TODO Fetch futures in tasks and then wait/fetch on the tasks

#Get stats for the tasks
silostats = fetch.(dwfutures)
ndirs = getindex.(silostats, 1)
dagentstats = StructArray.(getindex.(silostats, 2))
fagentstats = StructArray.(getindex.(silostats, 3))

# Get results freom outputfutures
hitstampcounts = fetch.(outputfutures)
hitscounts = first.(hitstampcounts)
stampscounts = last.(hitstampcounts)

# Inform progress task that the show is over
put!(progress_channel, (;))
wait(progresstask)
wait(qstatustask)

#---
# Get stop time and compute elapsed

stop = now()
elapsed = canonicalize(stop - start)
@info "total elapsed time: $elapsed"
@info "output handler stats" hitscounts stampscounts
#=
@info "dir agent stats (per silo)"
println(dagentstats)
@info "file agent stats (per silo)"
println(fagentstats)
=#

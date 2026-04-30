using Dates
start = now(UTC)

using BLDirWalkers
using Distributed
using Sockets
using DelimitedFiles # For agent stats output files
import Base: n_avail # For status()

#---
# Start workers
#
# For BLUSE at MeerKAT, the hits and stamps files are archived under
# `/datag<N>/<hostname>/data/`.  On each compute/processing node this is
# symlinked as `/datag/<hostname>/data`.  Using a `topdirs` directory list that
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
#ENV["JULIA_DEBUG"] = "DirWalkers"

function start_workers(workerspec; prjdir=dirname(@__DIR__), kwargs...)
    addprocs(workerspec;
        dir="/tmp",
        env=[
            #"JULIA_DEBUG"=>"DirWalkers",
            "JULIA_PROJECT"=>prjdir
        ],
        exename=joinpath(Sys.BINDIR, Base.julia_exename()),
        exeflags="--threads=1",
        max_parallel=32,
        kwargs...
    )
end

silospecs = [
    [("blpn$i", :auto) for i in 0:15],      # /datag0 "silo"
    [("blpn$i", :auto) for i in 16:31],     # /datag1 "silo"
    [("blpn$i", :auto) for i in 32:47],     # /datag2 "silo"
    [("blpn$i", :auto) for i in 48:63],     # /datag3 "silo"
    [("blpn$i", :auto) for i in 64:2:78],   # /datag4 "silo"
    [("blpn$i", :auto) for i in 80:2:94],   # /datag4 "silo"
    [("blpn$i", :auto) for i in 96:2:110],  # /datag4 "silo"
    [("blpn$i", :auto) for i in 112:2:126], # /datag4 "silo"
]
#silospecs = [ # For TESTing at Berkeley data center
#    [("blpc3", 2)],
#    [("blpc3", 2)]
#]

@info "starting workers"
oversubcribe = 2
@time silows = map(silospecs) do hostspecs
    reduce(vcat, start_workers(hostspecs) for _ in 1:oversubcribe)
end

#=
# Create worker proc just for outq
outq_worker = start_workers(1;
    exename="/opt/julia/julia-1.11.2/bin/julia",
    exeflags="--threads=auto --gcthreads=8,1",
)|>only
=#
outq_worker = myid()

@info "$(sum(length, silows)) workers started"

#---
# Initialize workers

@info "get precompile workers"

function wmap(f, ws, args...; kwargs...)
    asyncmap(fetch,
        asyncmap(w->remotecall(f, w, args...; kwargs...), ws)
    )
end

function wmap(f, ws, args...; kwargs...)
    asyncmap(fetch,
        [remotecall(()->Base.eval(Main, :($f($(args...); $(kwargs...)))), pid) for pid in workers()]
    )
end

#=
function wmap_fetch(f, ws, args...; kwargs...)
    asyncmap(w->remotecall_fetch(f, w, args...; kwargs...), ws)
end
=#

# Get a representative worker for each unique CPU_NAME in the cluster
@time precompile_workers = Dict(wmap(()->Sys.CPU_NAME=>myid(), workers()))|>values|>collect

# Possibly precompile on the precompile workers (maybe have to loop?)
@everywhere precompile_workers using BLDirWalkers

@info "initialize workers"

@time @everywhere begin
    using DuckDB
    using DuckDB: DBInterface as DBI
    using StructArrays
    using BLDirWalkers
end
@info "workers initialized"

#---
# Set topdirs

#topdirs = ["/datag/blpn32"]
#topdirs = ["/scratch/yuri/blpn$i" for i in 0:63]
topdirs = ["/datag/blpn$i" for i in Iterators.flatten((0:63, 64:2:126))]
#topdirs = ["/datax/scratch/jwst-test"] # For TESTing at Berkeley data center

#---
# Create DirWalker queues.

# One dirq and one fileq on the first worker of each silo
dirqs = [RemoteDirQueue(ws[1]; sz=Inf) for ws in silows]
fileqs = [RemoteFileQueue(ws[1]; sz=Inf) for ws in silows]
#outq = RemoteOutQueue{Vector{<:Seticore.AbstractCapnpInfo}}(outq_worker; sz=500)
outq = RemoteOutQueue{NamedTuple}(outq_worker; sz=Inf)

# Status function to get available item counts for all queues
status(dirqs,fileqs,outq) = (n_avail.(dirqs), n_avail.(fileqs), n_avail(outq))

#---
# @everywhere function to create seticore DuckDB database and return its
# connection handle.

@everywhere function create_seticore_database(; dbfile="/scratch/tmp/seticorefiles_$(myid()).duckdb",
                                    memory_limit="6GiB",
                                    temp_directory="/buf0/seticoredb.tmp/$(myid())")
    mkpath(temp_directory)
    db = DBI.connect(DuckDB.DB)#, dbfile)
    DBI.execute(db,"set memory_limit='$memory_limit'")
    DBI.execute(db,"set temp_directory='$temp_directory'")

    appenders = map(("seticorehits", "seticorestamps"),
                    (Seticore.HitInfo, Seticore.StampInfo)) do tabname, tabtype
        DuckDB.register_data_frame(db, StructArray{tabtype}(undef, 0), "mock$tabname")
        DBI.execute(db, "create or replace table $tabname as select cast(1 as int64) as 'id', * from mock$tabname")
        DuckDB.unregister_table(db, "mock$tabname")
        DuckDB.Appender(db, tabname)
    end

    db, appenders...
end

#---
# @everywhere function to append item to table

@everywhere function appendrow(appender, id, item::T) where T <: Union{Seticore.HitInfo, Seticore.StampInfo}
    DuckDB.append(appender, id)
    for i in 1:fieldcount(T)
        DuckDB.append(appender, getfield(item, i))
    end
    DuckDB.end_row(appender)
end

#---
# @everywhere define process_seticore_files function

# This function is the main processing loop of a "seticore file agent" (i.e. a
# file agent that processes seticore files from a file queue).  Each seticore
# file agent starts by opening an in-memory DuckDB database and creating
# `seticorehits` and `seticorestamps` tables.

# To limit ram usage, each file agent limits DuckDB to a 1 GiB memory max.
# To prevent temp file conflicts, each file agent makes a worker-specific
# `/scratch/tmp/dirwalker.$(myid())` directory and sets DuckDb's
# "temp_directory" to that path.  These are the defaults for
# create_seticore_database.

@everywhere function process_seticore_files(filefunc, fileq, outq, id, args...;
    outdir="/datag/users/seticoredb/parquet", kwargs...
)
try
    start = time()
    nfiles = 0

    db, hitappender, stampappender = create_seticore_database()
    #dbfile = db.handle.file

    nhits = 0
    nstamps = 0
    hitid = id
    stampid = id
    idlo, idhi = extrema(workers())
    idstep = idhi - idlo + 1
    milestone = 100_000

    # Take from fileq until we get an empty string
    for file in Iterators.takewhile(!isempty, fileq)
        try
            @debug "processing file $file"
            for items in filefunc(file, args...; kwargs...)
                for item in items
                    if item isa Seticore.HitInfo
                        # Use hitid for id column
                        appendrow(hitappender, hitid, item)
                        hitid += idstep
                        nhits += 1
                    elseif item isa Seticore.StampInfo
                        # Use stampid for id column
                        appendrow(stampappender, stampid, item)
                        stampid += idstep
                        nstamps += 1
                    else
                        @warn "ignoring unexpected type $(typeof(item)) from $filefunc"
                    end
                end
            end

            nfiles += 1

            # Flush the appenders every so often and post progress to outq
            if nhits + nstamps > milestone
                milestone = nhits + nstamps + 100_000

                DuckDB.flush(hitappender)
                DuckDB.flush(stampappender)

                # Put non-final status into outq
                put!(outq, (; id, nfiles, nhits, nstamps, final=false))
            end
        catch ex
            @warn "got exception processing $file" ex
        end
    end

    # Recycle empty value for other tasks processing fileq (if any)
    put!(fileq, "")

    # Close appenders
    DuckDB.close(hitappender)
    DuckDB.close(stampappender)

    try
        # Save seticorehits and seticorestamps tables to worker-specfic parquet files
        for tab in ("seticorehits", "seticorestamps")
            DBI.execute(db, "COPY $tab TO '$outdir/$tab.$id.parquet' (FORMAT 'parquet', CODEC 'zstd')")
        end

        close(db)
    catch ex
        @warn "$(gethostname()) got exception writing to $outdir/$tab.$id.parquet" ex
    end

    # Put final status into outq
    put!(outq, (; id, nfiles, nhits, nstamps, final=true))

    return (; host=gethostname(), id, t=time()-start, n=nfiles)
catch ex
    return (; host=gethostname(), id, t=time()-start, n=ex)
end
end


#---
# Start DirWalkers, one per silo

# filefunc and filepred must be defined @everywhere!
runtasks = map(zip(silows, dirqs, fileqs)) do (ws, dirq, fileq)
    @spawnat ws[1] run_dirwalker(
        Seticore.filefunc, dirq, fileq, outq, topdirs;
        filepred=Seticore.filepred,
        dagentspec=ws[2:2:end],
        fagentspec=ws[3:2:end],
        extraspec=ws[2:2:end], # Repurpose dagent workers as fagent workers
        process_files=process_seticore_files
    )
end

#---
# Diagnostic/experiment

#=
silostats = run_dirwalker(
    Seticore.filefunc, dirqs[1], fileqs[1], outq, topdirs;
    filepred=Seticore.filepred,
    dagentspec=silows[1][1:2:end],
    fagentspec=silows[1][2:2:end],
    extraspec=silows[1][1:2:end] # Repurpose dagent workers as fagent workers
) .|> StructArray
# =#

#---
# Connect to database and create tables

#=
@info "create new database"
# connect to database (for main task)
dbfile = "/scratch/users/seticoredb/seticorefiles.duckdb"
#dbfile = "/datax/scratch/davidm/filedb/seticorefiles.duckdb" # TESTing
rm(dbfile; force=true)
db = DBI.connect(DuckDB.DB, dbfile)
DBI.execute(db,"set memory_limit='1GiB'")

hittab = "seticorehits"
stamptab = "seticorestamps"

@info "creating table $hittab for HitInfo records"
@info "creating table $stamptab for StampInfo records"

DuckDB.register_data_frame(db, StructArray{Seticore.HitInfo}(undef, 0), "mockhit")
DuckDB.register_data_frame(db, StructArray{Seticore.StampInfo}(undef, 0), "mockstamp")

DBI.execute(db, "create table $hittab as select cast(1 as int64) as 'id', * from mockhit")
DBI.execute(db, "create table $stamptab as select cast(1 as int64) as 'id', * from mockstamp")

DuckDB.unregister_table(db, "mockhit")
DuckDB.unregister_table(db, "mockstamp")
=#

#---
# Define appender functions

#=
function appendrow(appender, id::Threads.Atomic, item::T) where T <: Union{Seticore.HitInfo, Seticore.StampInfo}
    rowid = Threads.atomic_add!(id, 1)
    DuckDB.append(appender, rowid)
    for i in 1:fieldcount(T)
        DuckDB.append(appender, getfield(item, i))
    end
    DuckDB.end_row(appender)
end

function appendrows(appender, id::Threads.Atomic, itemvec::AbstractVector)
    for item in itemvec
        appendrow(appender, id, item)
    end
end

"""
For each AbstractCapnp item from `outq`, append a row to proper table in
database `db`.
"""
function run_appender(db, outq, npending_dws=Threads.Atomic{Int}(1), appid=1;
                      hitid::Threads.Atomic{Int}, stampid::Threads.Atomic{Int},
                      hittab="seticorehits", stamptab="seticorestamps", dirqs, fileqs)
    # Create Appenders append rows to database for each item in outq
    hitappender = DuckDB.Appender(db, hittab)
    stampappender = DuckDB.Appender(db, stamptab)

    @info "appender $appid writing records to database"
#=
    @info "appender $appid NOT writing records to database"
=#
    hitcount = 0
    stampcount = 0
    milestone = 100_000
    try
        for itemvec in outq
            if itemvec === nothing
                old_npending = Threads.atomic_sub!(npending_dws, 1)
                old_npending > 1 && continue

                # If we get here, then old_npending was 1 or less meaning that
                # we are all done!  Put nothing back into outq to signal any
                # other waiting threads to decrement/check npending
                put!(outq, nothing)
                break
            elseif isempty(itemvec)
                @warn "ignoring empty Vector from output queue"
            elseif itemvec[1] isa Seticore.HitInfo
                # Use hitid for id column
                appendrows(hitappender, hitid, itemvec)
                hitcount += length(itemvec)
            #elseif itemvec isa Vector{Seticore.StampInfo}
            elseif itemvec[1] isa Seticore.StampInfo
                # Use stampid for id column
                appendrows(stampappender, stampid, itemvec)
                stampcount += length(itemvec)
            else
                @warn "ignoring unexpected type $(typeof(itemvec[1])) from output queue"
            end

            # Flush the appenders every so often
            if (hitcount + stampcount) > milestone
                milestone = hitcount + stampcount + 100_000

                DuckDB.flush(hitappender)
                DuckDB.flush(stampappender)

                # appid 1 logs progress and checks memory pressure
                if appid == 1
                    elapsed = canonicalize(now(UTC)-start)
                    @info "found $(hitid[]-1) hits and $(stampid[]-1) stamps so far ($elapsed) $(status(dirqs,fileqs,outq))"
                    if Sys.free_memory() < Sys.total_memory() * 2/3
                        @info "memory threashold reached, calling GC.gc() on outq_worker ($(outq.where))"
                        pre_gc = @fetchfrom outq.where Base.gc_live_bytes()
                        t0 = now(UTC)
                        remotecall_fetch(GC.gc, outq.where, false)
                        gcelapsed = now(UTC) - t0
                        post_gc = @fetchfrom outq.where Base.gc_live_bytes()
                        reclaimed = Base.format_bytes(max(0, pre_gc-post_gc))
                        @info "reclaimed $reclaimed ($gcelapsed))"
                    end
                end
            end
        end
    finally
        DuckDB.close(hitappender)
        DuckDB.close(stampappender)
    end
    @info "appender $appid done writing records to database"

    hitcount, stampcount
end
=#

#---
# Run dirwalker database appender

#=
nouttasks = 16
hitid = Threads.Atomic{Int}(1)
stampid = Threads.Atomic{Int}(1)
outtasks = map(1:nouttasks) do appid
    Threads.@spawn run_appender(
        db, outq, npending_dws, appid; hitid, stampid, dirqs, fileqs
    )
end
#hitcount, stampcount = run_appender(db, outq, npending)
=#

#---
# Process outq

progressdict = Dict{Int, NTuple{3,Int}}()
outqiter = Iterators.takewhile(!isnothing, outq)
for (id, nfiles, nhits, nstamps, final) in outqiter
    nn = get(progressdict, id, (0,0,0)) .+ (nfiles, nhits, nstamps)
    progressdict[id] = nn
    tfiles, thits, tstamps = reduce((a,b)->a.+b, values(progressdict))
    println("worker $id: ($nfiles, $nhits, $nstamps); total: ($tfiles, $thits, $tstamps)")
end
nfiles, nhits, nstamps = reduce((a,b)->a.+b, values(progressdict))

#---
# Get stats for all runtasks

#stats = map(futures->fetch.(futures), fetch.(fetch(runtask)))
silostats = asyncmap(fetch, runtasks)
dagentstats = mapreduce(first, vcat, silostats) |> StructArray
fagentstats = mapreduce(last,  vcat, silostats) |> StructArray

#---
# Fetch from outtasks and remove the leftover `nothing` value from outq

#=
hitstampcounts = fetch.(outtasks)
#take!(outq)

# Unpack the values fetched from outtasks
hitcounts = first.(hitstampcounts)
stampcounts = last.(hitstampcounts)
hitcount = sum(hitcounts)
stampcount = sum(stampcounts)
=#

#---
# Get stop time and compute elapsed

timestamp = Dates.format(now(UTC), "YYYYmmddTHHMMSSZ")
stop = now(UTC)
elapsed = canonicalize(stop - start)
@info "total elapsed time: $elapsed"
@info "processed $nfiles files, created $nhits hit rows, $nstamps stamp rows"
dlogname = "dagentstats$(oversubcribe)x.$timestamp.txt"
flogname = "fagentstats$(oversubcribe)x.$timestamp.txt"
open(io->writedlm(io, eachrow(dagentstats)), dlogname, "w")
open(io->writedlm(io, eachrow(fagentstats)), flogname, "w")
@info "dir agent stats are in $dlogname"
@info "file agent stats are in $flogname"

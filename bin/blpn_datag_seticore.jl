using Dates
start = now()

using BLDirWalkers
using Distributed
using Sockets
using DuckDB, DataFrames
using DuckDB: DBInterface as DBI
using StructArrays
import Base: n_avail

#---
# Start workers
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

function start_workers(workerspec; prjdir=dirname(@__DIR__))
    addprocs(workerspec;
        # Hack for when running on blph0 (for now...)
        dir = pwd(),
        env = ["JULIA_PROJECT"=>prjdir],
        exeflags = "-t 1"
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
#@time ws = start_workers(workerspec)
@time silows = map(silospecs) do hostspecs
    reduce(vcat, start_workers(hostspecs) for _ in 1:oversubcribe)
end

@info "$(sum(length, silows)) workers started"

#---
# Initialize workers

@info "initalize workers"
@time @everywhere using BLDirWalkers
@info "workers initialized"

#---
# Set topdirs

topdirs = ["/datag/blpn$i" for i in Iterators.flatten((0:63, 64:2:126))]
#topdirs = ["/datax/scratch/jwst-test"] # For TESTing at Berkeley data center

#---
# Create DirWalker queues.
# dirq is non-remote, but fileq and outq are remote.

# Make RemoteDirQueues and RemoteFilesQueues on first worker of each silo
dirqs = [RemoteDirQueue(ws[1]; sz=Inf) for ws in silows]
fileqs = [RemoteFileQueue(ws[1]; sz=Inf) for ws in silows]
outq = RemoteOutQueue{Vector{<:Seticore.AbstractCapnpInfo}}(; sz=Inf)

#---
# Start DirWalkers

# filefunc and filepred must be defined @everywhere!
runtasks = map(zip(silows, dirqs, fileqs)) do (ws, dirq, fileq)
    @spawnat ws[1] run_dirwalker(
        Seticore.filefunc, dirq, fileq, outq, topdirs;
        filepred=Seticore.filepred,
        dagentspec=ws[1:1],
        fagentspec=ws[2:end],
        extraspec=[]
    )
end

#---
# Connect to database and create tables

@info "create new database"
# connect to database (for main task)
dbfile = "/datag/users/seticoredb/seticorefiles.duckdb"
#dbfile = "/datax/scratch/davidm/filedb/seticorefiles.duckdb" # TESTing
rm(dbfile; force=true)
db = DBI.connect(DuckDB.DB, dbfile)

hittab = "seticorehits"
stamptab = "seticorestamps"

@info "creating table $hittab for HitInfo records"
@info "creating table $stamptab for StampInfo records"

DuckDB.register_data_frame(db, StructArray{Seticore.HitInfo}(undef, 0), "mockhit")
DuckDB.register_data_frame(db, StructArray{Seticore.StampInfo}(undef, 0), "mockstamp")

DBI.execute(db, "create table $hittab as select 1 as 'id', * from mockhit")
DBI.execute(db, "create table $stamptab as select 1 as 'id', * from mockstamp")

DuckDB.unregister_table(db, "mockhit")
DuckDB.unregister_table(db, "mockstamp")

#---
# Define appender functions

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

#---
# Run dirwalker database appender

npending = length(runtasks)
hitcount, stampcount = run_appender(db, outq, npending)

#---
# Get stats for the tasks

#stats = map(futures->fetch.(futures), fetch.(fetch(runtask)))
silostats = map(stats->DataFrame.(stats), fetch.(runtasks))
dagentstats = mapreduce(first, vcat, silostats)
fagentstats = mapreduce(last, vcat, silostats)

#---
# Get stop time and compute elapsed

stop = now()
elapsed = canonicalize(stop - start)
@info "total elapsed time: $elapsed"
@info "created $(hitcount) hit rows, $(stampcount) stamp rows"
@info "dir agent stats (per silo)"
println(dagentstats)
@info "file agent stats (per silo)"
println(fagentstats)

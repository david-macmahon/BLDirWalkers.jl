using Dates
start = now()

using BLDirWalkers
using Distributed
using Sockets
using DuckDB, DataFrames
using DuckDB: DBInterface as DBI
using StructArrays
import Base: n_avail
#import Pkg

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

ENV["JULIA_WORKER_TIMEOUT"] = 120.0

function start_workers(workerspec; prjdir=dirname(@__DIR__))
    addprocs(workerspec;
        # Hack for when running on blph0 (for now...)
        dir = pwd(),
        env = ["JULIA_PROJECT"=>prjdir],
        exeflags = "-t 1"
    )
end

myhost = gethostname()
#workerspec = [("blpn$i", :auto) for i in 0:15]
workerspec = ["blpc3"] # For TESTing at Berkeley data center

@info "starting workers"
oversubcribe = 2
#@time ws = start_workers(workerspec)
@time ws = reduce(vcat, start_workers(workerspec) for _ in 1:oversubcribe)

@info "$(length(ws)) workers started"

#---
# Initialize workers

@info "initalize workers"
@time @everywhere using BLDirWalkers
@info "workers initialized"

#---
# Set topdirs

#topdirs = ["/datag0/bfr5_archive"]
topdirs = ["/datax/scratch/jwst-test"] # For TESTing at Berkeley data center

#---
# Create DirWalker queues.
# dirq is non-remote, but fileq and outq are remote.

dirq = DirQueue(Inf)
fileq = RemoteFileQueue(; sz=Inf)
outq = RemoteOutQueue{Vector{<:Seticore.AbstractCapnpInfo}}(; sz=Inf)

#---
# Start DirWalker

# filefunc (e.g. getheader) and filepred (e.g. isfilh5) must be defined
# @everywhere!
dagentspec = Sys.CPU_THREADS ÷ 2
fagentspec = workers()
extraspec = []
runtask = start_dirwalker(
    Seticore.filefunc, dirq, fileq, outq, topdirs;
    filepred=Seticore.filepred, dagentspec, fagentspec, extraspec
)

#---
# Connect to database and create table from first record

@info "create new database"
# connect to database (for main task)
#dbfile = "/datag0/bfr5_archive/bfr5files.duckdb"
dbfile = "/datax/scratch/davidm/filedb/seticorefiles.duckdb" # TESTing
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
function run_appender(db, outq; hittab="seticorehits", stamptab="seticorestamps")
    # Create Appenders append rows to database for each item in outq
    hitappender = DuckDB.Appender(db, hittab)
    stampappender = DuckDB.Appender(db, stamptab)

    @info "writing records to database"
    hitid = 0
    stampid = 0
    try
        for itemvec in outq
            if itemvec === nothing
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
    @info "done writing rows to database"

    hitid, stampid
end

#---
# Run dirwalker database appender

hitcount, stampcount = run_appender(db, outq)

#---
# Get stats for the tasks

#stats = map(futures->fetch.(futures), fetch.(fetch(runtask)))
dir_agent_stats, file_agent_stats = fetch(runtask) .|> DataFrame

#---
# Get stop time and compute elapsed

stop = now()
elapsed = canonicalize(stop - start)
@info "total elapsed time: $elapsed"
@info "created $(hitcount) hit rows, $(stampcount) stamp rows"
@info "dir agent stats"
println(dir_agent_stats)
@info "file agent stats"
println(file_agent_stats)

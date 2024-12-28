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

ENV["JULIA_WORKER_TIMEOUT"] = 120.0

function start_workers(workerspec; prjdir=dirname(@__DIR__))
    addprocs(workerspec;
        # Hack for when running on blph0 (for now...)
        dir = replace(pwd(), "mnt_home2"=>"home"),
        env = ["JULIA_PROJECT"=>prjdir],
        exeflags = "-t 1"
    )
end

myhost = gethostname()
#workerspec = ["blpc$i" for i in 0:3]
workerspec = [("blpc$i $(getaddrinfo("blpc$i"))", :auto) for i in 0:3 if "blpc$i" != myhost]
extraworkerspec = [("$myhost $(getaddrinfo(myhost))", :auto)]

@info "starting workers"
oversubcribe = 5
#@time ws = start_workers(workerspec)
@time ws = reduce(vcat, start_workers(workerspec) for _ in 1:oversubcribe)
@time xws = start_workers(extraworkerspec)

@info "$(length(ws))+$(length(xws)) workers started"

#---
# Initialize workers

@info "initalize workers"
@time @everywhere using BLDirWalkers
@info "workers initialized"

#---
# Set topdirs

topdirs = ["/datag/collate_mb", "/datag/pipeline"]

#---
# Create DirWalker queues.
# dirq is non-remote, but fileq and outq are remote.

dirq = DirQueue(Inf)
fileq = RemoteFileQueue(; sz=Inf)
outq = RemoteOutQueue{FBH5.Header}(; sz=Inf)

#---
# Start DirWalker

# filefunc (e.g. getheader) and filepred (e.g. isfilh5) must be defined
# @everywhere!
dagentspec = 4*Sys.CPU_THREADS
fagentspec = workers() ∩ ws # Take intersection in case some workers...
extraspec = workers() ∩ xws # ...were removed for misbehaving
runtask = start_dirwalker(
    FBH5.filefunc, dirq, fileq, outq, topdirs;
    filepred=FBH5.filepred, dagentspec, fagentspec, extraspec
)

#---
# Connect to database and create table from first record

@info "create new database"
# connect to database (for main task)
dbfile = "/datax/scratch/davidm/filedb/fbh5files.duckdb"
rm(dbfile; force=true)
db = DBI.connect(DuckDB.DB, dbfile)

tabname = "fbh5files"

@info "creating table $tabname for FBH5.Header records"
DuckDB.register_data_frame(db, StructArray{FBH5.Header}(undef, 0), "mockfilh5")
DBI.execute(db, "create table $tabname as select 1 as 'id', * from mockfilh5")
#DBI.execute(db, "alter table $tabname add primary key (id)")
DuckDB.unregister_table(db, "mockfilh5")

#---
# Define run_appender functions

"""
For each header from `outq`, append a row to table `tabname` in
database `db`.
"""
function run_appender(db, tabname, outq)
    # Create Appender and append a row to database for each file found by dw
    appender = DuckDB.Appender(db, tabname)

    @info "writing records to database"
    rowcount = 1
    try
        for row in outq
            row === nothing && break

            # Use rowcount for id column
            DuckDB.append(appender, rowcount)

            # Append columns for fields
            for i in 1:fieldcount(typeof(row))
                DuckDB.append(appender, getfield(row, i))
            end
            DuckDB.end_row(appender)

            rowcount += 1
            if rowcount % 100_000 == 0
                @info "found $rowcount files so far ($(now()-start))"
            end
        end
    finally
        DuckDB.close(appender)
    end
    @info "done writing rows to database"

    rowcount
end

#---
# Run dirwalker database appender

filecount = run_appender(db, tabname, outq)

#---
# Get stats for the tasks

dir_agent_stats, file_agent_stats = fetch(runtask) .|> DataFrame

#---
# Get stop time and compute elapsed

stop = now()
elapsed = canonicalize(stop - start)
@info "total elapsed time: $elapsed"
@info "created $(filecount) file rows"
@info "dir agent stats"
println(dir_agent_stats)
@info "file agent stats"
println(file_agent_stats)

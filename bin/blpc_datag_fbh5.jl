using Dates
start = now(UTC)

using BLDirWalkers
using BLDirWalkers: unixms, qstatus, workerhostpid
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

ENV["JULIA_WORKER_TIMEOUT"] = 120.0

#function start_workers(workerspec; prjdir=dirname(@__DIR__), exeflags="--threads=4")
function start_workers(workerspec; exeflags=`--project="$(dirname(@__DIR__))" --threads=4`, kwargs...)
    addprocs(workerspec;
        # Hack for when running on blph0 (for now...)
        #dir = replace(pwd(), "mnt_home2"=>"home"),
        exeflags, kwargs...
    )
end

myhost = gethostname()
#workerspec = ["blpc$i" for i in 0:3]
workerspec = [("blpc$i $(getaddrinfo("blpc$i.tenge.pvt"))", :auto) for i in 0:3]# if "blpc$i" != myhost]
#extraworkerspec = []#("$myhost $(getaddrinfo("$myhost.tenge.pvt"))", :auto)]

@info "starting workers"
oversubcribe = 2
#@time ws = start_workers(workerspec)
@time ws = reduce(vcat, start_workers(workerspec) for _ in 1:oversubcribe)

# Worker processes for file queue and out queue
dqworker, fqworker, oqworker = addprocs(3; exeflags=`--threads=auto`)

@info "$(length(ws))+3 workers started"

#---
# Initialize workers

@info "initalize workers"
@time @everywhere begin
    using Dates
    using BLDirWalkers
    using BLDirWalkers: unixms, qstatus, workerhostpid
    using DuckDB
    using DuckDB: DBInterface as DBI
    using StructArrays
    import Profile

    # Print profiling data to "$(unixms()).profile"
    Profile.peek_report[] = ()->Profile.print(joinpath(@__DIR__, "$(unixms()).profile"); groupby=:task)
end
@info "workers initialized"

#---
# Set topdirs, dbfile, and tablename

topdirs = ["/datag/collate_mb", "/datag/pipeline"]
dbfile = "/datax/scratch/davidm/filedb/fbh5files.duckdb"

#topdirs = ["/datag/public/bl_tess"]
#dbfile = "/datax/scratch/davidm/filedb/bl_tess.duckdb"

tablename = "fbh5rawfiles"

#---
# Create DirWalker remote queues.

# Each queue on a different worker
topq = RemoteTopQueue(; sz=Inf)
dirq = RemoteDirQueue(dqworker; sz=Inf)
fileq = RemoteFileQueue(fqworker; sz=Inf)
outq = RemoteOutQueue{FBH5.Header}(oqworker; sz=Inf)

#---
# Start DirWalker

# filefunc (e.g. getheader) and filepred (e.g. isfilh5) must be defined
# @everywhere!
viables = workers() ∩ ws # Take intersection in case some workers...
dagentspec = viables[1:2:end]
fagentspec = viables[2:2:end]

# Log worker hosts/PIDs
open(joinpath(@__DIR__, "workers.txt"), "w") do io
    println(io, "dir agents on ", workerhostpid(dagentspec[1]))
    println(io, "file agent on ", workerhostpid(fagentspec[1]))
    println(io, "dirq on ", workerhostpid(dqworker))
    println(io, "fileq on ", workerhostpid(fqworker))
    println(io, "outq on ", workerhostpid(oqworker))
end

dwtask = Threads.@spawn run_dirwalker(
    FBH5.filefunc, topq, dirq, fileq, outq, topdirs;
    filepred=FBH5.filepred, dagentspec, fagentspec, extraspec=dagentspec
)

#---
# Define output_handler functions

# The output_handler runs remotely so we have to define it `@everywhere` after
# the remote workers have started.

@everywhere begin
"""
    output_handler(dbfile, tablename, outq)

Create database in `dbfile` with table `tablename`.  Then for each header from
`outq`, append a row to table `tablename`.
"""
function output_handler(dbfile, tablename, outq)
    start = now(UTC) # For elapsed time messages

    # Connect to database and create table from first record
    @info "create new database"
    # connect to database (for main task)
    rm(dbfile; force=true)
    db = DBI.connect(DuckDB.DB, dbfile)

    @info "creating table $tablename for FBH5.Header records"
    DuckDB.register_data_frame(db, StructArray{FBH5.Header}(undef, 0), "mockfilh5")
    DBI.execute(db, "create table $tablename as select 1 as 'id', * from mockfilh5")
    #DBI.execute(db, "alter table $tablename add primary key (id)")
    DuckDB.unregister_table(db, "mockfilh5")

    # Create Appender and append a row to database for each file handled by dw
    appender = DuckDB.Appender(db, tablename)

    @info "writing records to database"
    rowcount = 0
    try
        for row in Iterators.takewhile(!isnothing, outq)
            # Increment rowcount
            rowcount += 1

            # Use rowcount for id column
            DuckDB.append(appender, rowcount)

            # Append columns for fields
            for i in 1:fieldcount(typeof(row))
                DuckDB.append(appender, getfield(row, i))
            end
            DuckDB.end_row(appender)

            if rowcount % 100_000 == 0
                @info "handled $rowcount files so far ($(now(UTC)-start))"
            end
        end
    finally
        DuckDB.close(appender)
    end
    @info "done writing rows to database"

    rowcount
end
end # @everywhere

#---
# Run dirwalker database appender on oqworker

# Run output_handler on out queue worker
outputfuture = @spawnat oqworker output_handler(dbfile, tablename, outq)

# Start task to fetch results when the output bhandler finishes.  Having a task
# for this allows `monitor_queues` to check only tasks for their "done" state.
dbtask = Threads.@spawn fetch(outputfuture)

#---
# Collect queue status every so often until done

function monitor_queues(top, dir, file, out, tasks...)
    t0 = unixms()
    stats = NamedTuple{(:time, :top, :dir, :file, :out)}[]
    dones = istaskdone.(tasks)
    while !all(dones)
        s = qstatus(; time=unixms()-t0, top, dir, file, out)
        println(s)
        push!(stats, s)
        sleep(10)
        dones = istaskdone.(tasks)
    end
    stats
end

statstask = Threads.@spawn monitor_queues(topq, dirq, fileq, outq, dwtask, dbtask)

#---
# Get stats for the tasks

ndirs, dstats, fstats = fetch(dwtask) .|> (identity, StructArray, StructArray)

filecount = fetch(dbtask)

stats = fetch(statstask)

#---
# Get stop time and compute elapsed time

stop = now(UTC)
elapsed = canonicalize(stop - start)
@info "total elapsed time: $elapsed"
@info "created $(filecount) file rows"
#=
@info "dir agent stats"
println(dstats)
@info "file agent stats"
println(fstats)
=#

using Dates
start = now(UTC)

using BLDirWalkers
using Distributed
#using Sockets
using DuckDB, DataFrames
using DuckDB: DBInterface as DBI
using StructArrays
#import Base: n_avail

#---
# Set topdirs

topdirs = ["/datax", "/datax2", "/datax3"]
ntop = length(topdirs)

#---
# Worker declarations

# We are totally I/O bound so start as many workers as we want to on each node.
# Be sure to start at least one more than ntop because we will use a separate
# dir worker on each node for each topdir.  We also use a dirq so that dir
# workers are not limited to the filesystem of their original dir.  This is
# starting to sound more like the original motivation for DirWalkers in the
# first place.  For now we let only a dir worker to recursively process a
# its topdir.  This is to parallelize the population of fileqs across
# filesystems.
# NB: it is assumed that topdirs are on different filesystems.
workers_per_node = ntop + 32

#workerspecs = [
#    ("blc00", workers_per_node),
#    ("bls0", workers_per_node)
#]
workerspecs = [
    [("blc$i$j", workers_per_node) for i in 0:2 for j in 0:7];
    [("blc3$i", workers_per_node) for i in 0:2];
    [("bls$i", workers_per_node) for i in 0:5]
]

ENV["JULIA_WORKER_TIMEOUT"] = 120.0

function start_workers(workerspecs; prjdir=dirname(@__DIR__))
    addprocs(workerspecs;
        dir = "/tmp",
        env = [
            "JULIA_PROJECT" => prjdir,
            "JULIA_CPU_TARGET" => ENV["JULIA_CPU_TARGET"]
        ],
        exeflags = "-t 1",
        max_parallel = length(workerspecs) * workers_per_node
    )
end

#---
# Start workers

@info "starting workers"

# all_ws is a single vector of all workers
@time all_ws = start_workers(workerspecs)

# Worker PIDs may not be assigned in an orderly way, so we query the workers for
# their hostnames so we can collate them by host.
@time worker_hosts = remotecall_fetch.(gethostname, all_ws)

# Get vector of workers for each worker host
host_ws = map(h->all_ws[worker_hosts.==h], first.(workerspecs))

# First ntop workers on each host is a dir worker
dir_ws = first.(host_ws, ntop)

# File workers are non-first-ntop workers of each host
file_ws = [
    ws[ntop+1:end] for ws in host_ws
]

@info "$(length(all_ws)) workers started"

#---
# Initialize workers

@info "initialize workers"
@time @everywhere all_ws[1] using BLDirWalkers # precompile?
@time @everywhere begin
    using BLDirWalkers

    # Single worker dirloop (no dirq)
    function dirloop(fileq, dir)
        # Get subdirs and raw/fbh5 files of dir
        contents = readdir(dir, join=true)
        subdirs = filter(isdir, contents)
        files = filter(f->isfile(f) && FBH5.israwfilh5(f), contents)

        # Put files into fileq
        for file in files
            put!(fileq, file)
        end

        # Recurse subdirs
        for subdir in subdirs
            # Don't let one bad subdir spoil the rest
            try
                dirloop(fileq, subdir)
            catch ex
                # Should "never" happen so log it!
                msg = hasfield(typeof(ex), :msg) ? ex.msg : string(ex)
                @error "$(gethostname()): $msg" _module=nothing _file=nothing
            end
        end
    end

    # Multi-worker file loop
    function fileloop(fileq, outq)
        # Take filenames until we get an empty string
        for fname in Iterators.takewhile(!isempty, fileq)
            try
                put!(outq, FBH5.get_header(fname))
            catch ex
                # Should "never" happen so log it!
                msg = hasfield(typeof(ex), :msg) ? ex.msg : string(ex)
                @error "$(gethostname()): $msg" _module=nothing _file=nothing
            end
        end

        # Put empty string into fileq for any remaining workers
        put!(fileq, "")
    end

end
@info "workers initialized"

#---
# Create queues.

# Create a fileq on first dir worker of each host
fileqs = map(dir_ws) do dws
    RemoteChannel(()->Channel{String}(Inf), first(dws))
end

# Single outq on main process
outq = RemoteOutQueue{FBH5.Header}(; sz=Inf)

#---
# Start dirloop on each dir_workers of each host for each topdir

dir_futures = map(dir_ws, fileqs) do dws, fileq
    map(dws, topdirs) do dw, topdir
        @spawnat dw dirloop(fileq, topdir)
    end
end

#---
# Start file_loop on file_workers

file_futures = map(file_ws, fileqs) do fws, fileq
    map(fws) do fw
        @spawnat fw fileloop(fileq, outq)
    end
end

#---
# Connect to database and create table from first record

@info "create new database"
# connect to database (for main task)
dbfile = "/datax/scratch/davidm/fbh5files.duckdb"
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
function run_appender(db, outq; tabname=tabname)
    # Create Appender and append a row to database for each file found by dw
    appender = DuckDB.Appender(db, tabname)

    @info "writing records to database"
    rowcount = 1
    try
        for row in Iterators.takewhile(!isnothing, outq)
            # Use rowcount for id column
            DuckDB.append(appender, rowcount)

            # Append columns for fields
            for i in 1:fieldcount(typeof(row))
                DuckDB.append(appender, getfield(row, i))
            end
            DuckDB.end_row(appender)

            rowcount += 1
            if rowcount % 100_000 == 0
                @info "found $rowcount files so far ($(now(UTC)-start))"
            end
        end
    catch ex
        # Should "never" happen so log it!
        msg = hasfield(typeof(ex), :msg) ? ex.msg : string(ex)
        @error "$(gethostname()): $msg" _module=nothing _file=nothing
    finally
        DuckDB.close(appender)
    end
    @info "done writing rows to database"

    rowcount
end

#---
# Start database appender task

appender_task = Threads.@spawn run_appender(db, outq; tabname)

#---
# Wait for tasks to complete

# For each host, wait for all its dir workers to finish and then put empty
# srting into the corresponding fileq.
foreach(dir_futures, fileqs) do dfs, fq
    # Wait for all dir workers of current host
    # TODO catch exceptions from dir workers?
    foreach(wait, dfs)
    put!(fq, "")
end
@info "all dir workers done"

# Wait for all file workers to finish
foreach(file_futures) do ffs
    foreach(wait, ffs)
end
@info "all file workers done"

# At this point, all fileqs have an empty string in them.
# These should be cleaned up if re-using the fileqs, but we don't so we don't.

# Signal end of data
put!(outq, nothing)

# Wait for appender_task to return rowcount+1 (so we subtract one)
rowcount = fetch(appender_task) - 1

#---
# Get stats for the tasks

#dir_agent_stats, file_agent_stats = fetch(runtask) .|> DataFrame

#---
# Get stop time and compute elapsed

stop = now(UTC)
elapsed = canonicalize(stop - start)
@info "total elapsed time: $elapsed"
@info "created $(rowcount) file rows"
#@info "dir agent stats"
#println(dir_agent_stats)
#@info "file agent stats"
#println(file_agent_stats)

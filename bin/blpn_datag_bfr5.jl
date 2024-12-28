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
# For BLUSE at MeerKAT, the BFR5 files are archived in /datag0/bfr5_archive.
# This is a flat directory structure (i.e. no subdirectories), so only one dir
# agent is needed.  Recommend running this script on one of the "bank 0"
# processing nodes (i.e. `blpn0` through `blpn15`) with a single in-process
# diragent and multiple (:auto) fagent workers on all the "Bank 0" compute
# nodes.

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
workerspec = [("blpn$i", :auto) for i in 0:15]
#workerspec = ["blpc1"] # For testing at Berkeley data center

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

topdirs = ["/datag0/bfr5_archive"]
#topdirs = ["/home/davidm/bfr5"] # For testing at Berkeley data center

#---
# Create DirWalker queues.
# dirq is non-remote, but fileq and outq are remote.

dirq = DirQueue(Inf)
fileq = RemoteFileQueue(; sz=Inf)
outq = RemoteOutQueue{BFR5.File}(; sz=Inf)

#---
# Start DirWalker

# filefunc (e.g. getheader) and filepred (e.g. isfilh5) must be defined
# @everywhere!
dagentspec = 1
fagentspec = workers()
extraspec = []
runtask = start_dirwalker(
    BFR5.filefunc, dirq, fileq, outq, topdirs;
    filepred=BFR5.filepred, dagentspec, fagentspec, extraspec
)

#---
# Connect to database and create table from first record

@info "create new database"
# connect to database (for main task)
dbfile = "/datag0/bfr5_archive/bfr5files.duckdb"
#dbfile = "/datax/scratch/davidm/filedb/bfr5files.duckdb" # testing
rm(dbfile; force=true)
db = DBI.connect(DuckDB.DB, dbfile)

filetab = "bfr5files"
anttab = "bfr5ants"
beamtab = "bfr5beams"

@info "creating table $filetab for BFR5.Header records"
@info "creating table $anttab for BFR5.Ant records"
@info "creating table $beamtab for BFR5.Beam records"

DuckDB.register_data_frame(db, StructArray{BFR5.Header}(undef, 0), "mockfile")
DuckDB.register_data_frame(db, StructArray{BFR5.Ant}(undef, 0), "mockant")
DuckDB.register_data_frame(db, StructArray{BFR5.Beam}(undef, 0), "mockbeam")

DBI.execute(db, "create table $filetab as select 1 as 'id', * from mockfile")
DBI.execute(db, "create table $anttab as select 1 as 'fileid', 2 as 'antseq', * from mockant")
DBI.execute(db, "create table $beamtab as select 1 as 'fileid', 2 as 'beamseq', * from mockbeam")

DuckDB.unregister_table(db, "mockfile")
DuckDB.unregister_table(db, "mockant")
DuckDB.unregister_table(db, "mockbeam")

#---
# Define run_appender functions

"""
For each header from `outq`, append a row to table `tabname` in
database `db`.
"""
function run_appender(db, outq; filetab="bfr5files", anttab="bfr5ants", beamtab="bfr5beams")
    # Create Appender and append a row to database for each file found by dw
    fileappender = DuckDB.Appender(db, filetab)
    antappender = DuckDB.Appender(db, anttab)
    beamappender = DuckDB.Appender(db, beamtab)

    @info "writing records to database"
    fileid = 1
    try
        for row in outq
            row === nothing && break

            # Use fileid for id column
            DuckDB.append(fileappender, fileid)

            # Append columns for fields of row, which must be a BFR5.File
            for i in 1:fieldcount(BFR5.Header)
                DuckDB.append(fileappender, getfield(row.header, i))
            end
            DuckDB.end_row(fileappender)

            # Append rows to bfr5ants table
            for (antseq, ant) in enumerate(row.ants)
                # Use fileid for fileid column
                DuckDB.append(antappender, fileid)
                DuckDB.append(antappender, antseq)
                for i in 1:fieldcount(BFR5.Ant)
                    DuckDB.append(antappender, getfield(ant, i))
                end
                DuckDB.end_row(antappender)
            end

            # Append rows to bfr5beamw table
            for (beamseq, beam) in enumerate(row.beams)
                # Use fileid for fileid column
                DuckDB.append(beamappender, fileid)
                DuckDB.append(beamappender, beamseq)
                for i in 1:fieldcount(BFR5.Beam)
                    DuckDB.append(beamappender, getfield(beam, i))
                end
                DuckDB.end_row(beamappender)
            end

            fileid += 1
            if fileid % 100_000 == 0
                @info "found $fileid files so far ($(now()-start))"
            end
        end
    finally
        DuckDB.close(fileappender)
        DuckDB.close(antappender)
        DuckDB.close(beamappender)
    end
    @info "done writing rows to database"

    fileid
end

#---
# Run dirwalker database appender

rowcount = run_appender(db, outq)

#---
# Get stats for the tasks

#stats = map(futures->fetch.(futures), fetch.(fetch(runtask)))
dagent_results, fagent_results = fetch(runtask) .|> DataFrame

#---
# Get stop time and compute elapsed

stop = now()
elapsed = canonicalize(stop - start)
@info "total elapsed time: $elapsed"

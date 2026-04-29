using Dates
start = now()

using BLDirWalkers
#using Sockets
#using DuckDB, DataFrames
using DuckDB
using DuckDB: DBInterface as DBI
using StructArrays

#---
# Get list of BFR5 files

bfr5files = [
    filter(BFR5.isbfr5, readdir("/home/obs/bfr5_archive"; join=true));
    filter(BFR5.isbfr5, readdir("/home/obs/bfr5"; join=true))
]
#---
# Connect to database and create tables from BFR5 structs

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
# Define channel to send BFR5.Files between reader task and appender tasks

channel = Channel{Union{BFR5.File,Nothing}}(Inf)

#---
# Start reader task

readertask = Threads.@spawn begin
    nbad = 0
    ngood = 0
    for bfr5name in bfr5files
        try
            bfr5file = BFR5.get_bfr5file(bfr5name)
            put!(channel, bfr5file)
            ngood += 1
        catch e
            nbad += 1
            @info "$(typeof(e)) reading $bfr5name"
        end
    end
    put!(channel, nothing)
    @info "readertask done" ngood nbad
end

#---
# For each BFR5.File in channel (until `nothing`), append to database tables

# Create Appenders
fileappender = DuckDB.Appender(db, filetab)
antappender = DuckDB.Appender(db, anttab)
beamappender = DuckDB.Appender(db, beamtab)

@info "writing records to database"
filecount = 0
antcount = 0
beamcount = 0
try
    for file in Iterators.takewhile(!isnothing, channel)

        # Append header to bfr5files table
        # Use filecount for id column
        filecount += 1
        DuckDB.append(fileappender, filecount)
        # Append columns for fields of row, which must be a BFR5.File
        for i in 1:fieldcount(BFR5.Header)
            DuckDB.append(fileappender, getfield(file.header, i))
        end
        DuckDB.end_row(fileappender)

        # Append ants to bfr5ants table
        for (antseq, ant) in enumerate(file.ants)
            # Use filecount for fileid column
            DuckDB.append(antappender, filecount)
            DuckDB.append(antappender, antseq)
            for i in 1:fieldcount(BFR5.Ant)
                DuckDB.append(antappender, getfield(ant, i))
            end
            DuckDB.end_row(antappender)
            antcount += 1
        end

        # Append beams to bfr5beams table
        for (beamseq, beam) in enumerate(file.beams)
            # Use filecount for fileid column
            DuckDB.append(beamappender, filecount)
            DuckDB.append(beamappender, beamseq)
            for i in 1:fieldcount(BFR5.Beam)
                DuckDB.append(beamappender, getfield(beam, i))
            end
            DuckDB.end_row(beamappender)
            beamcount += 1
        end

        if filecount % 1_000 == 0
            @info "processed $filecount files so far ($(now()-start))"
        end
    end
finally
    DuckDB.close(fileappender)
    DuckDB.close(antappender)
    DuckDB.close(beamappender)
end

@info "done writing rows to database"

#---
# Get stop time and compute elapsed

stop = now()
elapsed = canonicalize(stop - start)
@info "total elapsed time: $elapsed"
@info "created $(filecount) file rows, $(antcount) ant rows, $(beamcount) beam rows"

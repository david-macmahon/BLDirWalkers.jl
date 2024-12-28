### Beamformer Recipe File (HDF5)

module BFR5

using HDF5

const BFR5_SUFFIX = ".bfr5"
#const HDF5_SUFFIX_RE = r"\.(h5|hdf5)$"

isbfr5(f) = endswith(f, BFR5_SUFFIX) #|| contains(f, HDF5_SUFFIX_RE)

# Database representation of single BFR5.Header looks loosely like this TOML:
#
# [bfr5files]
# id* = 1
# obsid = ...
# etc
# 
# [[bfr5ants]]
# fileid* = 1
# antseq* = 1
# number = 0
# name = "m000"
# [[bfr5ants]]
# fileid* = 1
# antseq* = 2
# number = 1
# name = "m001"
# [[etc...]]
#
# [[bfr5beams]]
# fileid* = 1
# beamseq* = 1
# src_name = ...
# ra = ...
# dec = ...
# [[bfr5beams]]
# fileid* = 1
# beamseq* = 2
# src_name = ...
# ra = ...
# dec = ...

struct Header
    # For bfr5file table
    obsid::String
    jd::Union{Missing, Float64}
    time::Union{Missing, Float64}
    phase_center_ra::Union{Missing, Float64}
    phase_center_dec::Union{Missing, Float64}
    fch1::Union{Missing, Float64}
    foff::Union{Missing, Float64}
    nchans::Union{Missing, Int}
    nbeams::Union{Missing, Int}
    nants::Union{Missing, Int}
    telescope_name::Union{Missing, String}
    hostname::String
    filename::String
end

struct Ant
    # For bfr5ants table
    number::Union{Missing, Int}
    name::Union{Missing, String}
end

Ant(_::Missing, _::Missing) = error("Ant needs at least name or number")

struct Beam
    # For bfr5beams table
    src_name::String
    ra::Float64
    dec::Float64
end

struct File
    header::Header
    ants::Union{Missing, Vector{Ant}}
    beams::Union{Missing, Vector{Beam}}
end

function bfr5ants(numbers, names)
    Ant.(numbers, names)
end

function bfr5ants(_::Missing, _::Missing)
    missing
end

function bfr5beams(names, ras, decs)
    Beam.(names, ras, decs)
end

function bfr5beams(_::Missing, _::Missing, _::Missing)
    missing
end

function bfr5get(h5, key, idxs...)
    haskey(h5, key) ? h5[key][idxs...] : missing
end

function get_bfr5file(bfr5name::String)
    h5open(bfr5name) do h5
        f1, f2 = bfr5get(h5, "obsinfo/freq_array", 1:2)
        File(
            Header(
                bfr5get(h5, "obsinfo/obsid")|>string,
                bfr5get(h5, "delayinfo/jds", 1),
                bfr5get(h5, "delayinfo/time_array", 1),
                bfr5get(h5, "obsinfo/phase_center_ra"),
                bfr5get(h5, "obsinfo/phase_center_dec"),
                f1,      # fch1
                f2 - f1, # foff
                bfr5get(h5, "diminfo/nchan"),
                bfr5get(h5, "diminfo/nbeams"),
                bfr5get(h5, "diminfo/nants"),
                bfr5get(h5, "telinfo/telescope_name"),
                gethostname(),
                bfr5name
            ),
            bfr5ants(
                bfr5get(h5, "telinfo/antenna_numbers"),
                bfr5get(h5, "telinfo/antenna_names")
            ),
            bfr5beams(
                bfr5get(h5, "beaminfo/src_names"),
                bfr5get(h5, "beaminfo/ras"),
                bfr5get(h5, "beaminfo/decs")
            )
        )
    end
end

# For use with `start_dirwalkers`
filepred = isbfr5
filefunc = get_bfr5file

end # module FBH5

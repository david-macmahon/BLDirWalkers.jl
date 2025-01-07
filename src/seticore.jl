### Seticore Capnp Hit and Stamp files

module Seticore

using SeticoreCapnp
using SeticoreCapnp: nodata_factory

const HITS_SUFFIX = ".hits"
const STAMPS_SUFFIX = ".stamps"

isseticore(f) = endswith(f, HITS_SUFFIX) || endswith(f, STAMPS_SUFFIX)

"""
Base type for `HitInfo` and `StampInfo`
"""
abstract type AbstractCapnpInfo end

"""
A superset of a `SeticoreCapnp.Hit`.
"""
@kwdef struct HitInfo <: AbstractCapnpInfo
    # NamedTuple(::Hit) fields
    frequency::Float64
    index::Int32
    driftSteps::Int32
    driftRate::Float64
    snr::Float32
    coarseChannel::Int32
    beam::Int32
    power::Float32
    incoherentPower::Float32
    sourceName::String
    fch1::Float64
    foff::Float64
    tstart::Float64
    tsamp::Float64
    ra::Float64
    dec::Float64
    telescopeId::Int32
    numTimesteps::Int32
    numChannels::Int32
    startChannel::Int32
    # Additional fields
    fileindex::Int64
    hostname::String
    filename::String
end

@kwdef struct StampInfo <: AbstractCapnpInfo
    # NamedTuple(::Stamp) fields
    seticoreVersion::String
    sourceName::String
    ra::Float64
    dec::Float64
    fch1::Float64
    foff::Float64
    tstart::Float64
    tsamp::Float64
    telescopeId::Int32
    coarseChannel::Int32
    fftSize::Int32
    startChannel::Int32
    numTimesteps::Int32
    numChannels::Int32
    numPolarizations::Int32
    numAntennas::Int32
    frequency::Float64
    index::Int32
    driftSteps::Int32
    driftRate::Float64
    snr::Float32
    beam::Int32
    power::Float32
    incoherentPower::Float32
    # Additional fields
    fileindex::Int64
    hostname::String
    filename::String
end

function load_seticorefile(::Type{Hit}, hostname, filename)
    reader = CapnpReader(SeticoreCapnp.nodata_index_factory, Hit, filename)
    map(reader) do (hit, fileindex)
        HitInfo(; NamedTuple(hit)..., fileindex, hostname, filename)
    end
end

function load_seticorefile(::Type{Stamp}, hostname, filename)
    reader = CapnpReader(SeticoreCapnp.nodata_index_factory, Stamp, filename)
    map(reader) do (stamp, fileindex)
        StampInfo(; NamedTuple(stamp)..., fileindex, hostname, filename)
    end
end

function load_seticorefile(filename)
    if endswith(filename, HITS_SUFFIX)
        T = Hit
    elseif endswith(filename, STAMPS_SUFFIX)
        T = Stamp
    else
        error("unsupported extension: $filename")
    end

    load_seticorefile(T, gethostname(), filename)
end

# For use with `start_dirwalkers`
filepred = isseticore
filefunc = load_seticorefile

end # module Seticore

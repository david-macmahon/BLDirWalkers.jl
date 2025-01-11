### Seticore Capnp Hit and Stamp files

module Seticore

using SeticoreCapnp
using SeticoreCapnp: nodata_factory

const HITS_SUFFIX = ".hits"
const STAMPS_SUFFIX = ".stamps"

isseticore(f) = endswith(f, HITS_SUFFIX) || endswith(f, STAMPS_SUFFIX)

"""
Type alias for `Union{Missing,T}`
"""
Nullable{T} = Union{Missing,T}

"""
Base type for `HitInfo` and `StampInfo`
"""
abstract type AbstractCapnpInfo end

"""
A superset of a `SeticoreCapnp.Hit`.
"""
@kwdef struct HitInfo <: AbstractCapnpInfo
    # NamedTuple(::Hit) fields
    frequency::Nullable{Float64}
    index::Nullable{Int32}
    driftSteps::Nullable{Int32}
    driftRate::Nullable{Float64}
    snr::Nullable{Float32}
    coarseChannel::Nullable{Int32}
    beam::Nullable{Int32}
    power::Nullable{Float32}
    incoherentPower::Nullable{Float32}
    sourceName::Nullable{String}
    fch1::Nullable{Float64}
    foff::Nullable{Float64}
    tstart::Nullable{Float64}
    tsamp::Nullable{Float64}
    ra::Nullable{Float64}
    dec::Nullable{Float64}
    telescopeId::Nullable{Int32}
    numTimesteps::Nullable{Int32}
    numChannels::Nullable{Int32}
    startChannel::Nullable{Int32}
    # Additional fields
    fileindex::Nullable{Int64}
    hostname::Nullable{String}
    filename::Nullable{String}
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
    frequency::Nullable{Float64}
    index::Nullable{Int32}
    driftSteps::Nullable{Int32}
    driftRate::Nullable{Float64}
    snr::Nullable{Float32}
    beam::Nullable{Int32}
    power::Nullable{Float32}
    incoherentPower::Nullable{Float32}
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

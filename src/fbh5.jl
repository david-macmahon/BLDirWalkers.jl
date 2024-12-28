### Filterbank/HDF5 headers

module FBH5

using Blio, HDF5

const FIL_SUFFIX = ".fil"
const HDF5_SUFFIX_RE = r"\.(h5|hdf5|fbh5|hdf)$"

isfilh5(f) = endswith(f, FIL_SUFFIX) || contains(f, HDF5_SUFFIX_RE)

# Define FilH5Header struct to ensure that columns ordering is consistent
struct Header
    az_start::Union{Missing,Float64}
    data_size::Union{Missing,Int64}
    data_type::Union{Missing,Int32}
    fch1::Union{Missing,Float64}
    foff::Union{Missing,Float64}
    ibeam::Union{Missing,Int32}
    machine_id::Union{Missing,Int32}
    nbeams::Union{Missing,Int32}
    nbits::Union{Missing,Int64}
    nchans::Union{Missing,Int64}
    nfpc::Union{Missing,Int32}
    nifs::Union{Missing,Int64}
    nsamps::Union{Missing,Int64}
    rawdatafile::Union{Missing,String}
    source_name::Union{Missing,String}
    src_dej::Union{Missing,Float64}
    src_raj::Union{Missing,Float64}
    telescope_id::Union{Missing,Int32}
    tsamp::Union{Missing,Float64}
    tstart::Union{Missing,Float64}
    za_start::Union{Missing,Float64}
    hostname::String
    filename::String
end

function Header(; az_start=missing, data_size=missing, data_type=missing,
    fch1=missing, foff=missing, ibeam=missing, machine_id=missing,
    nbeams=missing, nbits=missing, nchans=missing, nfpc=missing, nifs=missing,
    nsamps=missing, rawdatafile=missing, source_name=missing, src_dej=missing,
    src_raj=missing, telescope_id=missing, tsamp=missing, tstart=missing,
    za_start=missing, hostname, filename, kwargs... #= unknown kwargs ignored =#
)
    Header(az_start, data_size, data_type, fch1, foff, ibeam, machine_id,
        nbeams, nbits, nchans, nfpc, nifs, nsamps, rawdatafile, source_name,
        src_dej, src_raj, telescope_id, tsamp, tstart, za_start, hostname,
        filename
    )
end

function convert_attribute(::Type{Union{Missing,T}}, v) where T
    if v isa AbstractString && T <: Number
        if v == "None"
            return missing
        elseif v == "False"
            return 0
        elseif v == "True"
            return 1
        else
            return parse(T, v)
        end
    end
    return v
end

function get_h5header(fname)
    try
        fieldmap = Dict(string.(fieldnames(Header)).=>fieldtypes(Header))
        h5open(fname) do h5
            data = h5["data"]
            attrs = attributes(data)
            pairs = []
            for k in keys(attrs)
                haskey(fieldmap, k) || continue
                v = attrs[k][]
                v isa Array && continue
                push!(pairs, Symbol(k) => convert_attribute(fieldmap[k], v))
            end
            #if !haskey(attrs, "nfpc")
            #    foff = convert_attribute(fieldmap["foff"], attrs["foff"][])
            #    # Compute nfpc for Green Bank files as Int32 to match FBH5's nfpc type
            #    push!(pairs, :nfpc => round(Int32, 187.5/64/abs(foff)))
            #end
            elsize = sizeof(eltype(data))
            push!(pairs, :data_size => elsize * prod(size(data)))
            push!(pairs, :nsamps => size(data, ndims(data)))
            sort(pairs, by=first)
            push!(pairs, :hostname => gethostname())
            push!(pairs, :filename => abspath(fname))
            NamedTuple(pairs)
        end
    catch
        (; hostname=gethostname(), filename=abspath(fname))
    end
end

function get_fbheader(fbname)
    try
        fbh = open(io->read(io, Filterbank.Header), fbname)
        # Compute nfpc for Green Bank files as Int32 to match FBH5's nfpc type
        fbh[:nfpc] = round(Int32, 187.5/64/abs(fbh[:foff]))
        # Delete redundant fields to match FBH5 headers
        delete!(fbh, :barycentric)
        delete!(fbh, :header_size)
        delete!(fbh, :pulsarcentric)
        delete!(fbh, :sample_size)
        # Add hostname and filename fields
        fbh[:hostname] = gethostname()
        fbh[:filename] = abspath(fbname)
        NamedTuple(fbh)
    catch
        (; hostname=gethostname(), filename=abspath(fbname))
    end
end


function get_header(fname)
    nt = endswith(fname, FIL_SUFFIX) ? get_fbheader(fname) : get_h5header(fname)
    Header(; nt...)
end

# For use with `start_dirwalkers`
filepred = isfilh5
filefunc = get_header

end # module FBH5

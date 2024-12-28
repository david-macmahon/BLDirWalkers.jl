# BLDirwalkers

This package extends the [`DirWalkers.jl`](../DirWalkers.jl) package to work
with Breakthrough Listen files and directory structures.  It currently has
support for running `DirWalkers` to extract metadata from Filterbank/HDF5 files
(such as produced at Green Bank and Parkes) as well as Beamformer Recipe files
(such as produced by BLUSE on MeerKAT).  The metadata is then stored in a DuckDB
database file.

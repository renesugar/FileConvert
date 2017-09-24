FileConvert
===========

FileConvert converts between CSV/TSV and Parquet files (schemas supported by the Arrow library).

# Table of Contents

- [Links](#links)
- [License](#license)
- [Building](#building)
- [Using](#using)

# Links

- [Documentation](https://www.mapd.com/docs/)
- [MapD GitHub](https://github.com/mapd/)

# License

This project is licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).

The repository includes a number of third party packages provided under separate licenses. Details about these packages and their respective licenses is at [ThirdParty/licenses/index.md](ThirdParty/licenses/index.md).

# Building

FileConvert is built using the same build process as MapD Core.

See MapD Core [README](https://github.com/mapd/mapd-core/blob/master/README.md)

# Using

The basic command line to convert a file from one type to another is:

FileConvert -i ./file.csv -o ./file.parquet

FileConvert -i ./file.parquet -o ./file1.csv


Tools from [parquet-cpp](https://github.com/apache/parquet-cpp) can be used to verify Parquet files generated.

parquet_reader --only-metadata ./file.parquet

parquet-scan ./file.parquet


The other command line options can be seen by typing:

FileConvert --help

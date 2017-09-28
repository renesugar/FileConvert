FileConvert
===========

FileConvert converts between CSV/TSV and Parquet files (schemas supported by the Arrow library).

# Table of Contents

- [Links](#links)
- [License](#license)
- [Building](#building)
- [Development](#development)
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

```
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=debug ..
make
```

# Development

There are several `make` targets that can be used to run development tools.

* Runs `cpplint` through the build system:

```
make lint
```

* Runs `clang-format` and updates files in place:

```
make format
```

* Runs `clang-format` and returns an error if any files need to be reformatted:

```
make check-format
```

* Runs `clang-tidy` and updates files in place:

```
make clang-tidy
```

* Runs `clang-tidy` and returns an error if any errors are found:

```
make check-clang-tidy
```

* After building, you can run the [infer](http://fbinfer.com/docs/getting-started.html) static analysis tool.

```
brew update
brew install infer
```

Make targets have been added to run the **infer** steps.

Other ways to use **infer** can be found in the article [Recommended flow for CI](http://fbinfer.com/docs/steps-for-ci.html).


    1. First run the capture step using:

    make infer

    2. Next, run the analyze step using:

    make infer-analyze

    (Note: The *analyze* step can take a very long time.)

    3. Next, run the report step using:

    make infer-report


# Using

(See https://issues.apache.org/jira/browse/PARQUET-1114 for versions of Arrow and Parquet-Cpp with necessary fixes.)

The basic command line to convert a file from one type to another is:

```
FileConvert -i ./file.csv -o ./file.parquet

FileConvert -i ./file.parquet -o ./file1.csv
```

Tools from [parquet-cpp](https://github.com/apache/parquet-cpp) can be used to verify Parquet files generated.

```
parquet-dump-schema ./file.parquet

parquet_reader --only-metadata ./file.parquet

parquet-scan ./file.parquet
```

The other command line options can be seen by typing:

```
FileConvert --help
```

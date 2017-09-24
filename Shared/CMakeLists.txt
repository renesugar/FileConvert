set(shared_source_files
    Datum.cpp
    timegm.cpp
    SanitizeName.cpp
    HalfFloat.cpp
)

add_library(Shared ${shared_source_files})

add_library_unity(duckdb_protocode_extensions
                  OBJECT
                  extensions.pb.cc)


set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_protocode_extensions> PARENT_SCOPE)


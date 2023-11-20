add_subdirectory(extensions)

add_library_unity(duckdb_protocode
                  OBJECT
                  algebra.pb.cc
		type.pb.cc
		capabilities.pb.cc
		extended_expression.pb.cc
		function.pb.cc
		parameterized_types.pb.cc
		plan.pb.cc
		type_expressions.pb.cc
		  )

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_protocode>
    PARENT_SCOPE)

add_library_unity(
  duckdb_operator_join
  OBJECT
  physical_blockwise_nl_join.cpp
  physical_comparison_join.cpp
  physical_cross_product.cpp
  physical_delim_join.cpp
  physical_hash_join.cpp
  physical_join.cpp
  physical_merge_sip_join.cpp
  physical_nested_loop_join.cpp
  physical_piecewise_merge_join.cpp
  physical_sip_join.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_join>
    PARENT_SCOPE)

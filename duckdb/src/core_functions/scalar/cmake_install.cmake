# Install script for directory: /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/src/core_functions/scalar

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/usr/local")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "Release")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Install shared libraries without execute permission?
if(NOT DEFINED CMAKE_INSTALL_SO_NO_EXE)
  set(CMAKE_INSTALL_SO_NO_EXE "0")
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for each subdirectory.
  include("/home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/src/core_functions/scalar/bit/cmake_install.cmake")
  include("/home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/src/core_functions/scalar/blob/cmake_install.cmake")
  include("/home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/src/core_functions/scalar/date/cmake_install.cmake")
  include("/home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/src/core_functions/scalar/enum/cmake_install.cmake")
  include("/home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/src/core_functions/scalar/generic/cmake_install.cmake")
  include("/home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/src/core_functions/scalar/list/cmake_install.cmake")
  include("/home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/src/core_functions/scalar/map/cmake_install.cmake")
  include("/home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/src/core_functions/scalar/math/cmake_install.cmake")
  include("/home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/src/core_functions/scalar/operators/cmake_install.cmake")
  include("/home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/src/core_functions/scalar/random/cmake_install.cmake")
  include("/home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/src/core_functions/scalar/string/cmake_install.cmake")
  include("/home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/src/core_functions/scalar/struct/cmake_install.cmake")
  include("/home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/src/core_functions/scalar/union/cmake_install.cmake")
  include("/home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/src/core_functions/scalar/debug/cmake_install.cmake")

endif()


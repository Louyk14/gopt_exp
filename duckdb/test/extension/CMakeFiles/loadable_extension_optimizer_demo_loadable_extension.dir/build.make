# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.17

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Disable VCS-based implicit rules.
% : %,v


# Disable VCS-based implicit rules.
% : RCS/%


# Disable VCS-based implicit rules.
% : RCS/%,v


# Disable VCS-based implicit rules.
% : SCCS/s.%


# Disable VCS-based implicit rules.
% : s.%


.SUFFIXES: .hpux_make_needs_suffix_list


# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake3

# The command to remove a file.
RM = /usr/bin/cmake3 -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb

# Include any dependencies generated for this target.
include test/extension/CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/depend.make

# Include the progress variables for this target.
include test/extension/CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/progress.make

# Include the compile flags for this target's objects.
include test/extension/CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/flags.make

test/extension/CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/loadable_extension_optimizer_demo.cpp.o: test/extension/CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/flags.make
test/extension/CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/loadable_extension_optimizer_demo.cpp.o: test/extension/loadable_extension_optimizer_demo.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object test/extension/CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/loadable_extension_optimizer_demo.cpp.o"
	cd /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/test/extension && /bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/loadable_extension_optimizer_demo.cpp.o -c /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/test/extension/loadable_extension_optimizer_demo.cpp

test/extension/CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/loadable_extension_optimizer_demo.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/loadable_extension_optimizer_demo.cpp.i"
	cd /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/test/extension && /bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/test/extension/loadable_extension_optimizer_demo.cpp > CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/loadable_extension_optimizer_demo.cpp.i

test/extension/CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/loadable_extension_optimizer_demo.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/loadable_extension_optimizer_demo.cpp.s"
	cd /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/test/extension && /bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/test/extension/loadable_extension_optimizer_demo.cpp -o CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/loadable_extension_optimizer_demo.cpp.s

# Object files for target loadable_extension_optimizer_demo_loadable_extension
loadable_extension_optimizer_demo_loadable_extension_OBJECTS = \
"CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/loadable_extension_optimizer_demo.cpp.o"

# External object files for target loadable_extension_optimizer_demo_loadable_extension
loadable_extension_optimizer_demo_loadable_extension_EXTERNAL_OBJECTS =

test/extension/loadable_extension_optimizer_demo.duckdb_extension: test/extension/CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/loadable_extension_optimizer_demo.cpp.o
test/extension/loadable_extension_optimizer_demo.duckdb_extension: test/extension/CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/build.make
test/extension/loadable_extension_optimizer_demo.duckdb_extension: test/extension/CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX shared library loadable_extension_optimizer_demo.duckdb_extension"
	cd /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/test/extension && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
test/extension/CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/build: test/extension/loadable_extension_optimizer_demo.duckdb_extension

.PHONY : test/extension/CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/build

test/extension/CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/clean:
	cd /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/test/extension && $(CMAKE_COMMAND) -P CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/cmake_clean.cmake
.PHONY : test/extension/CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/clean

test/extension/CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/depend:
	cd /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/test/extension /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/test/extension /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/test/extension/CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : test/extension/CMakeFiles/loadable_extension_optimizer_demo_loadable_extension.dir/depend


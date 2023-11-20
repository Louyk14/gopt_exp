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
include third_party/miniz/CMakeFiles/duckdb_miniz.dir/depend.make

# Include the progress variables for this target.
include third_party/miniz/CMakeFiles/duckdb_miniz.dir/progress.make

# Include the compile flags for this target's objects.
include third_party/miniz/CMakeFiles/duckdb_miniz.dir/flags.make

third_party/miniz/CMakeFiles/duckdb_miniz.dir/miniz.cpp.o: third_party/miniz/CMakeFiles/duckdb_miniz.dir/flags.make
third_party/miniz/CMakeFiles/duckdb_miniz.dir/miniz.cpp.o: third_party/miniz/miniz.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object third_party/miniz/CMakeFiles/duckdb_miniz.dir/miniz.cpp.o"
	cd /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/third_party/miniz && /bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/duckdb_miniz.dir/miniz.cpp.o -c /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/third_party/miniz/miniz.cpp

third_party/miniz/CMakeFiles/duckdb_miniz.dir/miniz.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/duckdb_miniz.dir/miniz.cpp.i"
	cd /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/third_party/miniz && /bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/third_party/miniz/miniz.cpp > CMakeFiles/duckdb_miniz.dir/miniz.cpp.i

third_party/miniz/CMakeFiles/duckdb_miniz.dir/miniz.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/duckdb_miniz.dir/miniz.cpp.s"
	cd /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/third_party/miniz && /bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/third_party/miniz/miniz.cpp -o CMakeFiles/duckdb_miniz.dir/miniz.cpp.s

# Object files for target duckdb_miniz
duckdb_miniz_OBJECTS = \
"CMakeFiles/duckdb_miniz.dir/miniz.cpp.o"

# External object files for target duckdb_miniz
duckdb_miniz_EXTERNAL_OBJECTS =

third_party/miniz/libduckdb_miniz.a: third_party/miniz/CMakeFiles/duckdb_miniz.dir/miniz.cpp.o
third_party/miniz/libduckdb_miniz.a: third_party/miniz/CMakeFiles/duckdb_miniz.dir/build.make
third_party/miniz/libduckdb_miniz.a: third_party/miniz/CMakeFiles/duckdb_miniz.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX static library libduckdb_miniz.a"
	cd /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/third_party/miniz && $(CMAKE_COMMAND) -P CMakeFiles/duckdb_miniz.dir/cmake_clean_target.cmake
	cd /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/third_party/miniz && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/duckdb_miniz.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
third_party/miniz/CMakeFiles/duckdb_miniz.dir/build: third_party/miniz/libduckdb_miniz.a

.PHONY : third_party/miniz/CMakeFiles/duckdb_miniz.dir/build

third_party/miniz/CMakeFiles/duckdb_miniz.dir/clean:
	cd /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/third_party/miniz && $(CMAKE_COMMAND) -P CMakeFiles/duckdb_miniz.dir/cmake_clean.cmake
.PHONY : third_party/miniz/CMakeFiles/duckdb_miniz.dir/clean

third_party/miniz/CMakeFiles/duckdb_miniz.dir/depend:
	cd /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/third_party/miniz /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/third_party/miniz /home/admin/louyunkai.lyk/gopt/gopt_experiment/duckdb/third_party/miniz/CMakeFiles/duckdb_miniz.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : third_party/miniz/CMakeFiles/duckdb_miniz.dir/depend


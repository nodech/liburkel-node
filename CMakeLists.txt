cmake_minimum_required(VERSION 3.4)
project(nurkel LANGUAGES C)

include(deps/liburkel/cmake/AppendCCompilerFlag.cmake)
include(NodeJS)

set(nurkel_cflags)
set(nurkel_defines)
set(nurkel_libs)

if(MSVC)
  append_c_compiler_flag(nurkel_cflags /wd4244
                                       /wd4267)
else()
  append_c_compiler_flag(nurkel_cflags -Wall
                                       -Wextra
                                       -Wcast-align
                                       -Wshadow)
endif()

add_subdirectory(deps/liburkel)
list(APPEND nurkel_libs urkel_static)

add_node_module(nurkel src/nurkel.c
                       src/util.c
                       src/common.c
                       src/transaction.c
                       src/tree.c)
target_compile_definitions(nurkel PRIVATE ${nurkel_defines})
target_compile_options(nurkel PRIVATE ${nurkel_cflags})
target_link_libraries(nurkel PRIVATE ${nurkel_libs})

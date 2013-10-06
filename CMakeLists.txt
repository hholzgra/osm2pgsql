PROJECT(osm2pgsql)
SET(VERSION 0.83.0)

CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

INCLUDE (CheckFunctionExists)
INCLUDE (GNUInstallDirs)



SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}")

FIND_PACKAGE(LibXml2 REQUIRED)
include_directories(${LIBXML2_INCLUDE_DIR})

FIND_PACKAGE(PostgreSQL REQUIRED)
include_directories(${PostgreSQL_INCLUDE_DIRS})

FIND_PACKAGE(ZLIB REQUIRED)
include_directories(${ZLIB_INCLUDE_DIRS})

FIND_PACKAGE(BZip2 REQUIRED)
include_directories(${BZIP2_INCLUDE_DIRS})

FIND_PACKAGE(LibGeos REQUIRED)
include_directories(${LIBGEOS_INCLUDE_DIR})

FIND_PACKAGE(LibProj REQUIRED)
include_directories(${LIBPROJ_INCLUDE_DIR})

FIND_PACKAGE(LibProj REQUIRED)
include_directories(${LIBPROJ_INCLUDE_DIR})

FIND_PACKAGE(Lua51)
if (LUA51_FOUND)
  include_directories(${LUA_INCLUDE_DIR})
  set(HAVE_LUA 1)
endif (LUA51_FOUND)

CHECK_FUNCTION_EXISTS(lseek64 HAVE_LSEEK64)

########### config.h ###############

include_directories(${CMAKE_CURRENT_BINARY_DIR})

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/cmake-config.h.in ${CMAKE_CURRENT_BINARY_DIR}/config.h)

########### osm2pgsql ###############

SET(osm2pgsql_SRCS
   UTF8sanitizer.c
   binarysearcharray.c
   binarysearcharray.h
   build_geometry.cpp
   build_geometry.h
   expire-tiles.c
   expire-tiles.h
   input.c
   input.h
   keyvals.c
   keyvals.h
   middle-pgsql.c
   middle-pgsql.h
   middle-ram.c
   middle-ram.h
   middle.h
   node-persistent-cache.c
   node-persistent-cache.h
   node-ram-cache.c
   node-ram-cache.h
   osm2pgsql.c
   osmtypes.h
   output-gazetteer.c
   output-gazetteer.h
   output-null.c
   output-null.h
   output-pgsql.c
   output-pgsql.h
   output.h
   parse-o5m.c
   parse-o5m.h
   parse-primitive.c
   parse-primitive.h
   parse-xml2.c
   parse-xml2.h
   pgsql.c
   pgsql.h
   rb.c
   rb.h
   reprojection.c
   reprojection.h
   sanitizer.h
   sprompt.c
   sprompt.h
   tagtransform.c
   tagtransform.h
   text-tree.c 
   text-tree.h
   wildcmp.c   
   wildcmp.h
)

add_executable(osm2pgsql ${osm2pgsql_SRCS})

target_link_libraries(osm2pgsql ${LIBXML2_LIBRARIES})
target_link_libraries(osm2pgsql ${PostgreSQL_LIBRARIES})
target_link_libraries(osm2pgsql ${ZLIB_LIBRARIES})
target_link_libraries(osm2pgsql ${BZIP2_LIBRARIES})
target_link_libraries(osm2pgsql ${GEOS_LIBRARIES})
target_link_libraries(osm2pgsql ${PROJ_LIBRARIES})
target_link_libraries(osm2pgsql ${LUA_LIBRARIES})

install(TARGETS osm2pgsql DESTINATION bin)


########### nodecachefilereader ###############

SET(nodecachefilereader_SRCS
   node-persistent-cache-reader.c
   node-persistent-cache.c
   node-ram-cache.c
   binarysearcharray.c
)

add_executable(nodecachefilereader ${nodecachefilereader_SRCS})
target_link_libraries(nodecachefilereader ${LIBXML2_LIBRARIES})
target_link_libraries(nodecachefilereader ${PostgreSQL_LIBRARIES})
target_link_libraries(nodecachefilereader ${ZLIB_LIBRARIES})
target_link_libraries(nodecachefilereader ${BZIP2_LIBRARIES})
target_link_libraries(nodecachefilereader ${GEOS_LIBRARIES})
target_link_libraries(nodecachefilereader ${PROJ_LIBRARIES})
target_link_libraries(nodecachefilereader ${LUA_LIBRARIES})

install(TARGETS nodecachefilereader DESTINATION bin)


########### install files ###############

install(FILES  default.style 900913.sql DESTINATION share/osm2pgsql)




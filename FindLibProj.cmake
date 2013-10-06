# - Try to find the Proj library
# Once done this will define
#
#  PROJ_FOUND - System has LibProj
#  PROJ_INCLUDE_DIR - The LibProj include directory
#  PROJ_LIBRARIES - The libraries needed to use LibProj

find_path(PROJ_INCLUDE_DIR NAMES proj_api.h
   )

find_library(PROJ_LIBRARIES proj
   )


# handle the QUIETLY and REQUIRED arguments and set LIBPROJ_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)

FIND_PACKAGE_HANDLE_STANDARD_ARGS(LibProj
                                  REQUIRED_VARS PROJ_LIBRARIES PROJ_INCLUDE_DIR
                                  )

mark_as_advanced(LIBPROJ_INCLUDE_DIR LIBPROJ_LIBRARIES)

# - Try to find the Geos library
# Once done this will define
#
#  GEOS_FOUND - System has LibGeos
#  GEOS_INCLUDE_DIR - The LibGeos include directory
#  GEOS_LIBRARIES - The libraries needed to use LibGeos

find_path(GEOS_INCLUDE_DIR NAMES geos/version.h
   PATH_SUFFIXES geos
   )

find_library(GEOS_LIBRARIES geos
   )


# handle the QUIETLY and REQUIRED arguments and set LIBGEOS_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)

FIND_PACKAGE_HANDLE_STANDARD_ARGS(LibGeos
                                  REQUIRED_VARS GEOS_LIBRARIES GEOS_INCLUDE_DIR
                                  )

mark_as_advanced(LIBGEOS_INCLUDE_DIR LIBGEOS_LIBRARIES)

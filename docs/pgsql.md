# PostgreSQL/PostGIS Output Backend #

The PostgreSQL backend is the original and the default output backend.

It requires that the given database has the ``postgis`` extension installed,
if any of the hstore specific command line options is given the ``hstore``
extension from ``commons`` also needs to be installed.
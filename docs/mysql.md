# MariaDB/MySQL Output Backend #

MariaDB and MySQL both support GIS data types. Spatial indexes are also
available on some storage engines. GIS functionality is available out
of the box, without having to add extensions or preparing databases
to enable GIS functionality beforehand.

Recent versions, while far from being as mighty as PostGIS, implement
a pretty good subset of the OpenGIS specification.

As MariaDB originally was a fork of MySQL their GIS support was essentially
the same in their respective 5.1.x versions. MariaDB then substantially
improved GIS support with MariaDB 5.2 (TODO: 5.3?). Oracle caught up with
MySQL 5.6, but chose a different implemntation route. On the SQL level
both essentially support the same feature set though.

The ``osm2pgsql`` backend requires the feature set of MariaDB 5.2 or
MySQL 5.6 as a minimum at runtime. Building may also succeed with
headers and libraries from an older version installed, but isn't
recommended.

TODO: details, links to documentation, really MariaDB 5.2 ...

As there's only an output module for these database products for now
``--slim`` mode requires to either use the pgsql middle layer (default)
or to use the ``--flat-nodes`` feature.

## Building with MariaDB or MySQL support ##

Support for this output in ``osm2pgsql`` is optional and needs to be
requested explicitly with ``WITH_MYSQL`` when invoking CMake. This
option works for both MySQL and MariaDB.

If MariaDB or MySQL is installed in a non-standard location you can
additionally set ``MYSQL_INCLUDE_DIR`` and ``MYSQL_LIBRARIES`` to
specify the correct include and lib paths.

## Usage ##

To request importing into a MariaDB and MySQL database you need to
use the ``--output=mysql`` command line option.

Database name, host, user and password are specified in the same
way as with the pgsql output backend.
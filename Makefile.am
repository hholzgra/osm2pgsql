ACLOCAL_AMFLAGS = -I m4

bin_PROGRAMS = osm2pgsql nodecachefilereader

osm2pgsql_SOURCES = \
  osm2pgsql.c \
  UTF8sanitizer.c \
  binarysearcharray.c binarysearcharray.h \
  build_geometry.cpp build_geometry.h \
  expire-tiles.c expire-tiles.h \
  input.c input.h \
  keyvals.c keyvals.h \
  middle-pgsql.c middle-pgsql.h \
  middle-ram.c middle-ram.h \
  middle.h \
  node-persistent-cache.c node-persistent-cache.h \
  node-ram-cache.c node-ram-cache.h \
  osmtypes.h \
  output-gazetteer.c output-gazetteer.h \
  output-helper.c output-helper.h \
  output-null.c output-null.h \
  pgsql.h \
  output-pgsql.c output-pgsql.h \
  output.h \
  parse-o5m.c parse-o5m.h \
  parse-primitive.c parse-primitive.h \
  parse-xml2.c parse-xml2.h \
  pgsql.c \
  rb.c rb.h \
  reprojection.c reprojection.h \
  sanitizer.h \
  sprompt.c sprompt.h \
  style-file.c style-file.h \
  tagtransform.c tagtransform.h\
  text-tree.c text-tree.h \
  wildcmp.c wildcmp.h 

nodecachefilereader_SOURCES = \
  binarysearcharray.c \
  node-persistent-cache.c \
  node-persistent-cache-reader.c \
  node-ram-cache.c 


if READER_PBF
osm2pgsql_SOURCES += \
  fileformat.pb-c.c fileformat.pb-c.h \
  osmformat.pb-c.c osmformat.pb-c.h\
  parse-pbf.c parse-pbf.h 

fileformat.pb-c.c: protobuf/fileformat.proto
	 $(AM_V_GEN) $(PROTOC_C) --proto_path=protobuf --c_out=. $<

fileformat.pb-c.h: fileformat.pb-c.c
	@if test ! -f $@; then \
	  rm -f $<; \
	  $(MAKE) $(AM_MAKEFLAGS) $<; \
	else :; fi

osmformat.pb-c.c: protobuf/osmformat.proto
	 $(AM_V_GEN) $(PROTOC_C) --proto_path=protobuf --c_out=. $<

osmformat.pb-c.h: osmformat.pb-c.c
	@if test ! -f $@; then \
	  rm -f $<; \
	  $(MAKE) $(AM_MAKEFLAGS) $<; \
	else :; fi

BUILT_SOURCES = \
  fileformat.pb-c.c fileformat.pb-c.h \
  osmformat.pb-c.c osmformat.pb-c.h

endif

if HAVE_MYSQL
osm2pgsql_SOURCES += \
  middle-mysql.c middle-mysql.h \
  mysql.c mysql.h \
  output-mysql.c output-mysql.h 
endif

osm2pgsqldir = $(datadir)/osm2pgsql

AM_CFLAGS = @PTHREAD_CFLAGS@ @LFS_CFLAGS@ @POSTGRESQL_CFLAGS@ @MYSQL_CFLAGS@ @XML2_CFLAGS@ @BZIP2_CFLAGS@  @GEOS_CFLAGS@ @PROJ_CFLAGS@  @PROTOBUF_C_CFLAGS@ -DOSM2PGSQL_DATADIR='"$(osm2pgsqldir)"' -DVERSION='"@PACKAGE_VERSION@"' @LUA_INCLUDE@
AM_CPPFLAGS = @PTHREAD_CFLAGS@ @POSTGRESQL_CFLAGS@ @XML2_CFLAGS@ @BZIP2_CFLAGS@  @GEOS_CFLAGS@ @PROJ_CFLAGS@ -DOSM2PGSQL_DATADIR='"$(osm2pgsqldir)"' -Igeos-fallback @LUA_INCLUDE@

AM_LDFLAGS = @PTHREAD_CFLAGS@ @ZLIB_LDFLAGS@ @ZLIB_LIBS@ @POSTGRESQL_LDFLAGS@ @POSTGRESQL_LIBS@ @MYSQL_LDFLAGS@ @XML2_LDFLAGS@ @BZIP2_LDFLAGS@ @BZIP2_LIBS@ @GEOS_LDFLAGS@ @GEOS_LIBS@ @PROJ_LDFLAGS@ @PROJ_LIBS@ @PROTOBUF_C_LDFLAGS@ @PROTOBUF_C_LIBS@ @LUA_LIB@

osm2pgsql_DATA = default.style 900913.sql

dist_man1_MANS = docs/osm2pgsql.1 docs/nodecachefilereader.1

EXTRA_DIST = osm2pgsql.spec.in \
             osm2pgsql.spec \
             protobuf/fileformat.proto \
             protobuf/osmformat.proto \
             debian \
             $(osm2pgsql_DATA)

$(PACKAGE).spec: $(PACKAGE).spec.in
	sed -e "s/@""PACKAGE""@/$(PACKAGE)/g; s/@""VERSION""@/$(VERSION)/g; s/@""SVN""@/`svnversion`/g;" $^ > $@

rpm: dist-gzip
	rpmbuild -ta $(distdir).tar.gz

distclean-local:
	@rm -f $(PACKAGE).spec
	@rm -f config.nice

test-pgsql: osm2pgsql
	@./osm2pgsql -H 127.0.0.1 -U gisuser --style=./default.style sample.pbf
	@psql -c "select count(*) from planet_osm_polygon;" gis gisuser

test-mysql: osm2pgsql
	@./osm2pgsql --output=mysql -H 127.0.0.1 -U gisuser --style=./default.style sample.pbf

#include "table-spatialite.hpp"

int table_spatialite_t::conn_count = 0;
sqlite3 *table_spatialite_t::sql_conn = nullptr;

#include "options.hpp"
#include "util.hpp"

#include <exception>
#include <algorithm>
#include <cstring>
#include <cstdio>
#include <utility>
#include <time.h>

#include <boost/algorithm/hex.hpp>

using std::string;
typedef boost::format fmt;

table_spatialite_t::table_spatialite_t(const database_options_t &database_options, const string& name, const string& type, const columns_t& columns, const hstores_t& hstore_columns,
    const int srid, const bool append, const bool slim, const bool drop_temp, const int hstore_mode,
    const bool enable_hstore_index, const boost::optional<string>& table_space, const boost::optional<string>& table_space_index) :
    table_t(database_options, name, type, columns, hstore_columns, srid, append,
   	    slim, drop_temp, hstore_mode, enable_hstore_index, table_space, table_space_index),
    column_names()
{
  point_fmt = fmt("POINT(%.15g %.15g)");
}

void table_spatialite_t::init_prepared_statements(void)
{
  std::string insert_sql_wkt = (fmt("INSERT INTO %1% (%2%) VALUES (%3% GeomFromText(?, %4%))") % name % column_names % bind_names % srid).str();
  
  if (SQLITE_OK != sqlite3_prepare_v2(sql_conn, insert_sql_wkt.c_str(), -1, &insert_stmt_wkt, 0)) {
    throw std::runtime_error((fmt("preparing stament '%1%' failed: %2%\n") % insert_sql_wkt % sqlite3_errmsg(sql_conn)).str());
  }
  
  std::string insert_sql_wkb = (fmt("INSERT INTO %1% (%2%) VALUES (%3% GeomFromWKB(?, %4%))") % name % column_names % bind_names % srid).str();
  
  if (SQLITE_OK != sqlite3_prepare_v2(sql_conn, insert_sql_wkb.c_str(), -1, &insert_stmt_wkb, 0)) {
    throw std::runtime_error((fmt("preparing stament '%1' failed: %2%\n") % insert_sql_wkb % sqlite3_errmsg(sql_conn)).str());
  }
  
}

table_spatialite_t::table_spatialite_t(const table_spatialite_t& other):
  table_t(other),
  column_names(other.column_names),
  bind_names(other.bind_names)
{
  init_prepared_statements();
  conn_count++;
}

table_spatialite_t::~table_spatialite_t()
{
    teardown();
}

void table_spatialite_t::teardown()
{
  if (conn_count > 0) {
    commit();
    
    if (--conn_count == 0) {
      if(sql_conn != nullptr) {
	sqlite3_close(sql_conn);
	sql_conn = nullptr;
      }
    }
  }

}

std::string const& table_spatialite_t::get_name() {
//TODO base classs?
    return name;
}

void table_spatialite_t::begin()
{
  if ((conn_count > 0) && (1 == sqlite3_get_autocommit(sql_conn))) {
    simple_query("BEGIN");
  }
}

void table_spatialite_t::commit()
{
  if ((conn_count > 0) && (0 == sqlite3_get_autocommit(sql_conn))) {
    simple_query("COMMIT");
    simple_query("BEGIN");
  }
}

void table_spatialite_t::start()
{
    connect();
    commit();
    
    fprintf(stderr, "Setting up table: %s\n", name.c_str());
    
    if (!append)
    {
      simple_query((fmt("DROP TABLE IF EXISTS %1%") % name).str());
    }

    /* These _tmp tables can be left behind if we run out of disk space */
    simple_query((fmt("DROP TABLE IF EXISTS %1%_tmp") % name).str());

    if (!append)
    {
        //define the new table
        string sql = (fmt("CREATE TABLE %1% (`osm_id` BIGINT") % name).str();

	//first with the regular columns
        for(columns_t::const_iterator column = columns.begin(); column != columns.end(); ++column) {
	  sql += (fmt(",`%1%` %2%") % column->first % column->second).str();
	}

	// TODO: we don't support hstore columns yet

	sql += ")";

	simple_query(sql);

	sql = (fmt("SELECT AddGeometryColumn('%1%', 'way', %2%, 'GEOMETRY', 'XY');") % name % srid).str();
	
	simple_query(sql);

	//slim mode needs this to be able to delete from tables in pending
	if (slim && !drop_temp) {
	  std::string sql = (fmt("CREATE INDEX %1%_pkey ON %1% (osm_id)") % name).str();
	  simple_query(sql);
	}
	
	simple_query((fmt("SELECT CreateSpatialIndex('%1%', 'way');") % name).str());
    } //appending
    else {
      // TODO: check the columns against those in the existing table
    }

    //generate column list for INSERT
    column_names = "`osm_id`,";

    //first with the regular columns
    bind_names = "?,";
    for(columns_t::const_iterator column = columns.begin(); column != columns.end(); ++column){
      column_names += (fmt("`%1%`,") % column->first).str();
      bind_names += "?,";
    }
    
    // TODO: then with the hstore columns
    // add tags column and geom column
    // if (hstore_mode != HSTORE_NONE)
    //    cols += "tags,";

    column_names += "`way`";

    init_prepared_statements();
    
    begin();
}

void table_spatialite_t::stop()
{
  commit();

  teardown();
}

void table_spatialite_t::write_node(const osmid_t id, const taglist_t &tags, double lat, double lon)
{
    write_row(id, tags, (point_fmt % lon % lat).str());
}

void table_spatialite_t::delete_row(const osmid_t id)
{
  simple_query((del_fmt % name % id).str());
}

void table_spatialite_t::write_row(const osmid_t id, const taglist_t &tags, const std::string &geom)
{
    int n=1, stat;

    sqlite3_stmt *insert_stmt = (geom[0] > 'F') ? insert_stmt_wkt : insert_stmt_wkb;

    stat = sqlite3_bind_int(insert_stmt, n++, id);
    if (SQLITE_OK != stat) {
        throw std::runtime_error((fmt("bind_int failed: %1%\n") % stat).str());
    }

    for(columns_t::const_iterator column = columns.begin(); column != columns.end(); ++column) {
        int idx;

        if ((idx = tags.indexof(column->first)) >= 0)
        {
            if (SQLITE_OK != sqlite3_bind_text(insert_stmt, n++, tags[idx].value.c_str(), -1, SQLITE_STATIC)) {
                throw std::runtime_error((fmt("bind_text '%1%' failed: %2%\n") % tags[idx].value % sqlite3_errmsg(sql_conn)).str());
            }
        } else {
            if (SQLITE_OK != sqlite3_bind_null(insert_stmt, n++)) {
                throw std::runtime_error((fmt("bind_null failed '%1%' (%2%): %3%\n") % column->first % n % sqlite3_errmsg(sql_conn)).str());
            }
        }
    }

    // TODO hstore

    if (insert_stmt == insert_stmt_wkt) {
        sqlite3_bind_text(insert_stmt, n++, geom.c_str(), -1, SQLITE_STATIC); 
    } else {
        char *bin;
        int len = geom.length()/2;

        bin = (char *)malloc(len);
        boost::algorithm::unhex(geom, bin);

        sqlite3_bind_blob(insert_stmt, n++, bin, len, SQLITE_STATIC);
    }

    if (SQLITE_DONE != sqlite3_step(insert_stmt)) {
        throw std::runtime_error((fmt("step failed: %1%\n") % sqlite3_errmsg(sql_conn)).str());
    }
    if (SQLITE_OK != sqlite3_reset(insert_stmt)) {
        throw std::runtime_error((fmt("reset failed: %1%\n") % sqlite3_errmsg(sql_conn)).str());
    }
}


table_t::wkb_reader table_spatialite_t::get_wkb_reader(const osmid_t id)
{
  sqlite3_stmt *stmt;
  
  std::string sql = (fmt("SELECT way FROM %1% WHERE osm_id = %2%") % name % id).str();

  sqlite3_prepare_v2(sql_conn, sql.c_str(), sql.length() + 1, &stmt, NULL);

  return table_t::wkb_reader(new wkb_reader_impl(stmt));
}

void table_spatialite_t::connect()
{
  if (conn_count == 0) {
    sqlite3 *_conn;

    sqlite3_config(SQLITE_CONFIG_SERIALIZED);
    sqlite3_enable_shared_cache(1);
    
    if ( SQLITE_OK  != sqlite3_open(database_options.db.c_str(), &_conn)) {
        throw std::runtime_error((fmt("Connection to database failed: %1%\n") % sqlite3_errmsg(_conn)).str());
    }
    
    sql_conn = _conn;
    
    simple_query("SELECT load_extension('mod_spatialite.so');");
    simple_query("SELECT InitSpatialMetaData(1);");
    simple_query("PRAGMA read_uncommitted = True;");
  }

  conn_count++;
}


/* Escape data appropriate to the type */
void table_spatialite_t::escape_type(const string &value, const string &type, string& dst) {

    // For integers we take the first number, or the average if it's a-b
    if (type == "int4") {
        int from, to;
        int items = sscanf(value.c_str(), "%d-%d", &from, &to);
        if (items == 1)
            dst.append((single_fmt % from).str());
        else if (items == 2)
            dst.append((single_fmt % ((from + to) / 2)).str());
        else
            dst.append("\\N");
    }
        /* try to "repair" real values as follows:
         * assume "," to be a decimal mark which need to be replaced by "."
         * like int4 take the first number, or the average if it's a-b
         * assume SI unit (meters)
         * convert feet to meters (1 foot = 0.3048 meters)
         * reject anything else
         */
    else if (type == "real")
    {
        string escaped(value);
        std::replace(escaped.begin(), escaped.end(), ',', '.');

        float from, to;
        int items = sscanf(escaped.c_str(), "%f-%f", &from, &to);
        if (items == 1)
        {
            if (escaped.size() > 1 && escaped.substr(escaped.size() - 2).compare("ft") == 0)
                from *= 0.3048;
            dst.append((single_fmt % from).str());
        }
        else if (items == 2)
        {
            if (escaped.size() > 1 && escaped.substr(escaped.size() - 2).compare("ft") == 0)
            {
                from *= 0.3048;
                to *= 0.3048;
            }
            dst.append((single_fmt % ((from + to) / 2)).str());
        }
        else
            dst.append("\\N");
    }//just a string
    else {
      dst.append("'");
      for (const char c: value) {
	    switch(c) {
	    case '\'':  dst.append("\'\'"); break;
	    case '\\':  dst.append("\\\\"); break;
	    //case 8:   dst.append("\\\b"); break;
	    //case 12:  dst.append("\\\f"); break;
	    case '\n':  dst.append("\\\n"); break;
	    case '\r':  dst.append("\\\r"); break;
	    case '\t':  dst.append("\\\t"); break;
	    //case 11:  dst.append("\\\v"); break;
	    default:    dst.push_back(c); break;
            }
	}
      dst.append("'");
    }
}



void table_spatialite_t::simple_query(const std::string &sql)
{
  //  fprintf(stderr, "SQL: %s\n", sql.c_str());
  
  if (sqlite3_exec(sql_conn, sql.c_str(), 0, 0, 0)) {
    fprintf(stderr, "SQL Query: %s\n", sql.c_str());
    fprintf(stderr, "  Error %d: %s\n", sqlite3_errcode(sql_conn), sqlite3_errmsg(sql_conn));
    abort();
  }
}

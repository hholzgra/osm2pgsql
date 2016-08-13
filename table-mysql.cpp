#include "table-mysql.hpp"
#include "options.hpp"
#include "util.hpp"

#include <exception>
#include <algorithm>
#include <cstring>
#include <cstdio>
#include <utility>
#include <time.h>

using std::string;
typedef boost::format fmt;

table_mysql_t::table_mysql_t(const database_options_t &database_options, const string& name, const string& type, const columns_t& columns, const hstores_t& hstore_columns,
    const int srid, const bool append, const bool slim, const bool drop_temp, const int hstore_mode,
    const bool enable_hstore_index, const boost::optional<string>& table_space, const boost::optional<string>& table_space_index) :
    table_t(database_options, name, type, columns, hstore_columns, srid, append,
   	    slim, drop_temp, hstore_mode, enable_hstore_index, table_space, table_space_index),
    sql_conn(nullptr),
    column_names(),
    hstore_type("")
{
  point_fmt = fmt("ST_GeomFromText('POINT(%.15g %.15g)')");
}

table_mysql_t::table_mysql_t(const table_mysql_t& other):
  table_t(other),
  sql_conn(nullptr),
  column_names(other.column_names),
  hstore_type(other.hstore_type)
{
    if (other.sql_conn) {
        connect();
    }
}

table_mysql_t::~table_mysql_t()
{
    teardown();
}

void table_mysql_t::teardown()
{
    if(sql_conn != nullptr)
    {
        mysql_close(sql_conn);
        sql_conn = nullptr;
    }
}

std::string const& table_mysql_t::get_name() {
    return name;
}

void table_mysql_t::begin()
{
    simple_query("BEGIN");
}

void table_mysql_t::commit()
{
    fprintf(stderr, "Committing transaction for %s\n", name.c_str());
    mysql_commit(sql_conn);
}

void table_mysql_t::start()
{
    if(sql_conn)
        throw std::runtime_error(name + " cannot start, its already started");

    connect();
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
        string sql = (fmt("CREATE TABLE %1% (`osm_id` BIGINT,") % name).str();

	//first with the regular columns
        for(columns_t::const_iterator column = columns.begin(); column != columns.end(); ++column) {
	  sql += (fmt("`%1%` %2%,") % column->first % column->second).str();
	}

        //then with the hstore columns
        for(hstores_t::const_iterator hcolumn = hstore_columns.begin(); hcolumn != hstore_columns.end(); ++hcolumn)
            sql += (fmt("`%1%` %s%,") % (*hcolumn) % hstore_type).str();
	
        //add tags column
	if (hstore_mode != HSTORE_NONE) {
            sql += "`tags` " + hstore_type + ",";
	}

	sql += "way geometry NOT NULL, SPATIAL INDEX(way)) ENGINE=MyISAM";

	simple_query(sql);
	
	//slim mode needs this to be able to delete from tables in pending
        if (slim && !drop_temp) {
            sql = (fmt("CREATE INDEX %1%_pkey ON %1% (osm_id) USING BTREE ") % name).str();
            simple_query(sql);
        }

	simple_query((fmt("ALTER TABLE %1% DISABLE KEYS") % name).str());
    } //appending
    else {
      // TODO: check the columns against those in the existing table
    }

    //generate column list for INSERT
    column_names = "`osm_id`,";
    
    //first with the regular columns
    for(columns_t::const_iterator column = columns.begin(); column != columns.end(); ++column) {
        column_names += (fmt("`%1%`,") % column->first).str();
    }
    
    if (hstore_mode != HSTORE_NONE) {
        column_names += "`tags`,";
    }

    column_names += "`way`";
}

void table_mysql_t::stop()
{
    simple_query((fmt("ALTER TABLE %1% ENABLE KEYS") % name).str());  
    teardown();
}

void table_mysql_t::write_node(const osmid_t id, const taglist_t &tags, double lat, double lon)
{
    write_row(id, tags, (point_fmt % lon % lat).str());  
}

void table_mysql_t::delete_row(const osmid_t id)
{
    simple_query((del_fmt % name % id).str());
}

void table_mysql_t::write_row(const osmid_t id, const taglist_t &tags, const std::string &geom)
{
    std::string sql = (fmt("INSERT INTO %1% (%2%) VALUES ( %3%, ") % name % column_names % id).str();

    // used to remember which columns have been written out already.
    std::vector<bool> used;

    if (hstore_mode != HSTORE_NONE) {
        used.assign(tags.size(), false);
    }
    
    for(columns_t::const_iterator column = columns.begin(); column != columns.end(); ++column)
    {
        int idx;
        if ((idx = tags.indexof(column->first)) >= 0)
        {
	    escape_type(tags[idx].value, column->second, sql);
	    if (hstore_mode == HSTORE_NORM) {
	        used[idx] = true;
	    }
        } else {
	    sql += "NULL";
	}
	sql += ",";
    }

    for(hstores_t::const_iterator hstore_column = hstore_columns.begin(); hstore_column != hstore_columns.end(); ++hstore_column)
    {
        bool added = false;

        //iterate through the list of tags, first one is always null
        for (taglist_t::const_iterator xtags = tags.begin(); xtags != tags.end(); ++xtags)
	  {
            //check if the tag's key starts with the name of the hstore column
            if(xtags->key.compare(0, hstore_column->size(), *hstore_column) == 0)
	      {
                //generate the short key name, somehow pointer arithmetic works against the key string...
                const char* shortkey = xtags->key.c_str() + hstore_column->size();
		
                //and pack the shortkey with its value into the hstore
		// TODO support both variants
                if(added)
		  sql += "','";
		else
		  sql += "COLUMN_CREATE('";

		sql += shortkey; // TODO escape?
		sql += "','";

		{ 
		  int len = xtags->value.length() * 2 + 1;
		  
		  if (len) {
		    char *buf = (char *)malloc(len);
		    
		    mysql_real_escape_string(sql_conn, buf, xtags->value.c_str(), xtags->value.length());
		    
		    sql.append(buf);
		    
		    free(buf);
		  }
		}
		
                //we did at least one so we need commas from here on out
                added = true;
	      }
	  }
	
        //finish the column off
        //if you found not matching tags write a NUL
        if(added)
	  sql.append("'),");
	else
	  sql.append("NULL,");
    }

    if (hstore_mode != HSTORE_NONE) {
      bool added = false;
      for (size_t i = 0; i < tags.size(); ++i) {
	const tag_t& xtag = tags[i];
	//skip z_order tag and keys which have their own column
	if (used[i] || ("z_order" == xtag.key))
	  continue;
	
	if(added)
	  sql += "','";
	else
	  // TODO support both variants
	  sql += "COLUMN_CREATE('";
	
	sql += xtag.key;
	sql += "','";
	{ 
	  int len = xtag.value.length() * 2 + 1;
	  
	  if (len) {
	    char *buf = (char *)malloc(len);
	    
	    mysql_real_escape_string(sql_conn, buf, xtag.value.c_str(), xtag.value.length() );
	    
	    sql.append(buf);
	    
	    free(buf);
	  }
	}
	
	//we did at least one so we need commas from here on out
	added = true;
      }
      
      //finish the hstore column
      if(added)
	sql.append("'),");
      else
	sql.append("NULL,");
    }

    if (geom[0] == 'S') {
      // this is a ST_something function call already, no need to convert
      // usually from write_node() above
      sql += geom;
    } else if (geom == "01030000000100000000000000") {
      // TODO: find a better way to identify NULL geometries
      //       we need to convert these to an empty collection
      //       as MySQL spatial indexes don't support NULL values
      sql += "ST_GeomFromText('GEOMETRYCOLLECTION()')";
    } else {
      sql += "ST_GeomFromWKB(0x";
      sql += geom;
      sql += ")";
    }
    sql += ")";

    simple_query(sql);
}


table_t::wkb_reader table_mysql_t::get_wkb_reader(const osmid_t id)
{
    std::string sql = (fmt("SELECT way FROM %1% WHERE osm_id = %2%") % name % id).str();

    simple_query(sql);
    
    return table_t::wkb_reader(new wkb_reader_impl(mysql_store_result(sql_conn)));
}


void table_mysql_t::connect()
{
    MYSQL *_conn = (MYSQL *)calloc(sizeof(MYSQL), 1);
    const char *db   = database_options.db.c_str();
    const char *host = database_options.host     ? database_options.host->c_str()       : nullptr;
    const int   port = database_options.port     ? atoi(database_options.port->c_str()) : 0;
    const char *user = database_options.username ? database_options.username->c_str()   : nullptr;
    const char *pwd  = database_options.password ? database_options.password->c_str()   : nullptr;

    mysql_init(sql_conn);
    if (!mysql_real_connect(_conn, host, user, pwd, db, port, NULL, 0)) {
        throw std::runtime_error((fmt("Connection to database failed: %1%\n") % mysql_error(_conn)).str());
	free((void *)_conn); // TODO never reached?
    }

    sql_conn = _conn;

    simple_query("SET NAMES utf8");

    simple_query("DROP TABLE IF EXISTS _feature_test");
    if (0 == mysql_query(sql_conn, "CREATE TABLE _feature_test(id int primary key, foo json)")) {
      hstore_type = "JSON";
      simple_query("DROP TABLE IF EXISTS _feature_test");
    } else if (0 == mysql_query(sql_conn, "SELECT COLUMN_CREATE('a','b')")) {
      MYSQL_RES *res = mysql_store_result(sql_conn);
      mysql_free_result(res);
      hstore_type = "BLOB";
    }


    if (hstore_type.empty()) {
      if (hstore_mode != HSTORE_NONE) {
	 throw std::runtime_error((fmt("Database version %1% does not support --hstore") % mysql_get_server_info(sql_conn)).str());
      }
      if (hstore_columns.size() > 0) {
	 throw std::runtime_error((fmt("Style has hstore columns but database version %1% does not support that") % mysql_get_server_info(sql_conn)).str());	
      }
    }
}


void table_mysql_t::escape_type(const string &value, const string &type, string& dst) {
    // TODO: lots of code duplication in here, only the actual string escape part
    //       is DBMS specific ... move to table_t class and only keep string
    //       escaping in actual DBMS specific subclasses ...
    if (type == "int4") {
        int from, to;
        int items = sscanf(value.c_str(), "%d-%d", &from, &to);
        if (items == 1)
            dst.append((single_fmt % from).str());
        else if (items == 2)
            dst.append((single_fmt % ((from + to) / 2)).str());
        else
            dst.append("NULL");
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
        else {
            dst.append("NULL");
	}
    } else { //just a string
        int len = value.length() * 2 + 1;

	if (len) {
	  char *buf = (char *)malloc(len);
	  
	  mysql_real_escape_string(sql_conn, buf, value.c_str(), value.length());

	  dst.append("\"");
	  dst.append(buf);
	  dst.append("\"");
	  
	  free(buf);
	} else {
	  dst.append("");
	}
    }
}

void table_mysql_t::simple_query(std::string sql)
{
  if (mysql_query(sql_conn, sql.c_str())) {
    fprintf(stderr, "MySQL Query: %s\n", sql.c_str());
    fprintf(stderr, "  Error %d: %s\n", mysql_errno(sql_conn), mysql_error(sql_conn));
    abort();
  }
}

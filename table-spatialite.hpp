#ifndef TABLE_SPATIALITE_H
#define TABLE_SPATIALITE_H

#include "osmtypes.hpp"

#include <cstddef>
#include <string>
#include <vector>
#include <utility>
#include <memory>

#include <boost/optional.hpp>
#include <boost/format.hpp>

#include "table.hpp"

#include <sqlite3.h>

typedef std::vector<std::string> hstores_t;
typedef std::vector<std::pair<std::string, std::string> > columns_t;

class table_spatialite_t : public table_t
{
    public:
        table_spatialite_t(const database_options_t &database_options, const std::string& name, const std::string& type, const columns_t& columns, const hstores_t& hstore_columns, const int srid,
                const bool append, const bool slim, const bool droptemp, const int hstore_mode, const bool enable_hstore_index,
                const boost::optional<std::string>& table_space, const boost::optional<std::string>& table_space_index);
        table_spatialite_t(const table_spatialite_t& other);
        ~table_spatialite_t();

        table_spatialite_t *clone() { return new table_spatialite_t(*this); }
        void teardown();
  
        void start();
        void stop();

        void begin();
        void commit();

        void write_row(const osmid_t id, const taglist_t &tags, const std::string &geom);
        void write_node(const osmid_t id, const taglist_t &tags, double lat, double lon);
        void delete_row(const osmid_t id);

        std::string const& get_name();

        static int conn_count;
        static sqlite3 *sql_conn;

    protected:
  
        struct sqlite_result_closer
        {
            void operator() (sqlite3_stmt *stmt)
            {
                sqlite3_finalize(stmt);
            }

        };

    public:
        //interface from retrieving well known binary geometry from the table
        class wkb_reader_impl: public table_t::wkb_reader_impl
        {
            friend table_spatialite_t;
            public:
                const char* get_next()
                {
		     int stat;
		       
		     stat = sqlite3_step(m_stmt.get());
		     if (stat == SQLITE_ROW) {
		         return (const char *)sqlite3_column_text(m_stmt.get(), 0);
		     } else {
		         return nullptr;
		     }
		}
                int get_count() const
	        {
	  	    // TODO remove
		    return m_count;
		}
                void reset()
                {
		    // TODO implement ... but it is never used anyway?
                }
            private:
                wkb_reader_impl(sqlite3_stmt *stmt)
		  : m_stmt(stmt),
		    m_count(0 /* TODO */),
		    m_first(0 /* TODO */)
                {}

                std::unique_ptr<sqlite3_stmt, sqlite_result_closer> m_stmt;
                int m_count;
                int m_first;
        };
  
        table_t::wkb_reader get_wkb_reader(const osmid_t id);
  
    protected:
        std::string column_names, bind_names;
        sqlite3_stmt *insert_stmt_wkt;
        sqlite3_stmt *insert_stmt_wkb;
        void escape_type(const std::string &value, const std::string &type, std::string& dst);
        void connect();
        void simple_query(const std::string &sql);
        void init_prepared_statements(void);
};

#endif

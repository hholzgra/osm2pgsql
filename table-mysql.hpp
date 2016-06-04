#ifndef TABLE_MYSQL_H
#define TABLE_MYSQL_H

#include "osmtypes.hpp"

#include <cstddef>
#include <string>
#include <vector>
#include <utility>
#include <memory>

#include <boost/optional.hpp>
#include <boost/format.hpp>

#include <mysql/mysql.h>

#include "table.hpp"

typedef std::vector<std::string> hstores_t;
typedef std::vector<std::pair<std::string, std::string> > columns_t;

class table_mysql_t : public table_t
{
    public:
        table_mysql_t(const database_options_t &database_options, const std::string& name, const std::string& type, const columns_t& columns, const hstores_t& hstore_columns, const int srid,
                const bool append, const bool slim, const bool droptemp, const int hstore_mode, const bool enable_hstore_index,
                const boost::optional<std::string>& table_space, const boost::optional<std::string>& table_space_index);
        table_mysql_t(const table_mysql_t& other);
        ~table_mysql_t();

        table_mysql_t *clone() { return new table_mysql_t(*this); }
        void teardown();
  
        void start();
        void stop();

        void begin();
        void commit();

        void write_row(const osmid_t id, const taglist_t &tags, const std::string &geom);
        void write_node(const osmid_t id, const taglist_t &tags, double lat, double lon);
        void delete_row(const osmid_t id);

        std::string const& get_name();

    protected:
        MYSQL *sql_conn;
        struct mysql_result_closer
        {
            void operator() (MYSQL_RES *result)
            {
                mysql_free_result(result);
            }

        };

    public:
        //interface from retrieving well known binary geometry from the table
        class wkb_reader_impl: public table_t::wkb_reader_impl
        {
            friend table_mysql_t;
            public:
                const char* get_next()
                {
		    MYSQL_ROW row = mysql_fetch_row(m_result.get());
		    if (row) {
		        return row[0];
		    } else {
		        return nullptr;
		    }
                }
                int get_count() const
	        {
		    return m_count;
		}
                void reset()
                {
		  mysql_row_seek(m_result.get(), m_first);
                }
            private:
                wkb_reader_impl(MYSQL_RES *result)
		  : m_result(result),
		    m_count(mysql_num_rows(result)),
		    m_first(mysql_row_tell(result))
                {}

                std::unique_ptr<MYSQL_RES, mysql_result_closer> m_result;
                int m_count;
                MYSQL_ROW_OFFSET m_first;
        };
  
        table_t::wkb_reader get_wkb_reader(const osmid_t id);
  
    protected:
        std::string column_names;
        void escape_type(const std::string &value, const std::string &type, std::string& dst);
        void connect();
        void simple_query(std::string sql);
};

#endif

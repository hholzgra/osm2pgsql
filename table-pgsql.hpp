#ifndef TABLE_PGSQL_H
#define TABLE_PGSQL_H

#include "table.hpp"
#include <libpq-fe.h>

class table_pgsql_t : public table_t
{
    public:
  table_pgsql_t(const database_options_t &database_options, const std::string& name, const std::string& type, const columns_t& columns, const hstores_t& hstore_columns, const int srid,
                const bool append, const bool slim, const bool droptemp, const int hstore_mode, const bool enable_hstore_index,
                const boost::optional<std::string>& table_space, const boost::optional<std::string>& table_space_index);
        table_pgsql_t(const table_pgsql_t& other);
        ~table_pgsql_t();

        table_pgsql_t *clone() { return new table_pgsql_t(*this); }
  
        void start();
        void stop();

        void begin();
        void commit();

        void write_row(const osmid_t id, const taglist_t &tags, const std::string &geom);
        void write_node(const osmid_t id, const taglist_t &tags, double lat, double lon);
        void delete_row(const osmid_t id);

    protected:
	void connect();
        void stop_copy();
        void teardown();

        void write_columns(const taglist_t &tags, std::string& values, std::vector<bool> *used);
        void write_tags_column(const taglist_t &tags, std::string& values,
                               const std::vector<bool> &used);
        void write_hstore_columns(const taglist_t &tags, std::string& values);

        void escape4hstore(const char *src, std::string& dst);
        void escape_type(const std::string &value, const std::string &type, std::string& dst);

        pg_conn *sql_conn;
        bool copyMode;
        std::string buffer;
        std::string copystr;

        struct pg_result_closer
        {
            void operator() (PGresult* result)
            {
                PQclear(result);
            }

        };

    public:
        //interface from retrieving well known binary geometry from the table
        class wkb_reader_impl: public table_t::wkb_reader_impl
        {
            friend table_pgsql_t;
            public:
                const char* get_next()
                {
                    if (m_current < m_count) {
                        return PQgetvalue(m_result.get(), m_current++, 0);
                    }
                    return nullptr;
                }

                int get_count() const { return m_count; }
                void reset()
                {
                    //NOTE: PQgetvalue doc doesn't say if you can call it
                    //      multiple times with the same row col
                    m_current = 0;
                }
            private:
                wkb_reader_impl(PGresult* result)
                : m_result(result), m_count(PQntuples(result)), m_current(0)
                {}

                std::unique_ptr<PGresult, pg_result_closer> m_result;
                int m_count;
                int m_current;
        };
  
    table_t::wkb_reader get_wkb_reader(const osmid_t id);
};


#endif 

#ifndef TABLE_H
#define TABLE_H

#include "osmtypes.hpp"
#include "options.hpp"
#include <cstddef>
#include <string>
#include <vector>
#include <utility>
#include <memory>

#include <boost/optional.hpp>
#include <boost/format.hpp>

typedef std::vector<std::string> hstores_t;
typedef std::vector<std::pair<std::string, std::string> > columns_t;

class table_t
{
    public:
        table_t(const database_options_t& database_options, const std::string& name, const std::string& type, const columns_t& columns, const hstores_t& hstore_columns, const int srid,
                const bool append, const bool slim, const bool droptemp, const int hstore_mode, const bool enable_hstore_index,
                const boost::optional<std::string>& table_space, const boost::optional<std::string>& table_space_index);
        table_t(const table_t& other);
        ~table_t();

        virtual table_t *clone() = 0;
  
        std::string const& get_name();

        virtual void start() = 0;
        virtual void stop() = 0;

        virtual void begin() = 0;
        virtual void commit() = 0;

        virtual void write_row(const osmid_t id, const taglist_t &tags, const std::string &geom) = 0;
        virtual void write_node(const osmid_t id, const taglist_t &tags, double lat, double lon) = 0;
        virtual void delete_row(const osmid_t id) {};

        database_options_t database_options;
        std::string name;
        std::string type;
        std::string srid;
        bool append;
        bool slim;
        bool drop_temp;
        int hstore_mode;
        bool enable_hstore_index;
        columns_t columns;
        hstores_t hstore_columns;
        boost::optional<std::string> table_space;
        boost::optional<std::string> table_space_index;

        boost::format single_fmt, point_fmt, del_fmt;

    protected:
        class wkb_reader_impl 
        {
	    public:     
	        virtual ~wkb_reader_impl() { };
	        virtual const char *get_next() = 0;
	        virtual int get_count() const = 0;
	        virtual void reset() = 0;
        };

    public:
	class wkb_reader
        {
            public:
 	        wkb_reader(wkb_reader_impl* impl) : impl(impl) {};
	        ~wkb_reader() { delete impl; }
	        const char *get_next() { return impl->get_next(); };
	        int get_count() const { return impl->get_count();};
	        void reset() { impl->reset(); };
	    protected:
	        wkb_reader_impl *impl;
        };       

  virtual wkb_reader get_wkb_reader(const osmid_t id) = 0;

};


#endif

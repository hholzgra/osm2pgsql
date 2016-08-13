#include "table.hpp"
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

#define BUFFER_SEND_SIZE 1024


table_t::table_t(const database_options_t &database_options, const string& name, const string& type, const columns_t& columns, const hstores_t& hstore_columns,
    const int srid, const bool append, const bool slim, const bool drop_temp, const int hstore_mode,
    const bool enable_hstore_index, const boost::optional<string>& table_space, const boost::optional<string>& table_space_index) :
    database_options(database_options), name(name), type(type), srid((fmt("%1%") % srid).str()),
    append(append), slim(slim), drop_temp(drop_temp), hstore_mode(hstore_mode), enable_hstore_index(enable_hstore_index),
    columns(columns), hstore_columns(hstore_columns), table_space(table_space), table_space_index(table_space_index)
{
    //if we dont have any columns
    if(columns.size() == 0 && hstore_mode != HSTORE_ALL)
        throw std::runtime_error((fmt("No columns provided for table %1%") % name).str());

    //we use these a lot, so instead of constantly allocating them we predefine these
    single_fmt = fmt("%1%");
    point_fmt = fmt("POINT(%.15g %.15g)");
    del_fmt = fmt("DELETE FROM %1% WHERE osm_id = %2%");
}

table_t::table_t(const table_t& other):
    database_options(other.database_options), name(other.name), type(other.type), srid(other.srid),
    append(other.append), slim(other.slim), drop_temp(other.drop_temp), hstore_mode(other.hstore_mode), enable_hstore_index(other.enable_hstore_index),
    columns(other.columns), hstore_columns(other.hstore_columns), table_space(other.table_space),
    table_space_index(other.table_space_index), single_fmt(other.single_fmt), point_fmt(other.point_fmt), del_fmt(other.del_fmt)
{
}


table_t::~table_t()
{
}

std::string const& table_t::get_name() {
    return name;
}


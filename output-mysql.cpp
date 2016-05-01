#include "osmtypes.hpp"
#include "output-mysql.hpp"

struct middle_query_t;
struct options_t;

void output_mysql_t::cleanup() {
}

int output_mysql_t::start() {
    return 0;
}

void output_mysql_t::stop() {
}

void output_mysql_t::commit() {
}

void output_mysql_t::enqueue_ways(pending_queue_t &job_queue, osmid_t id, size_t output_id, size_t& added) {
}

int output_mysql_t::pending_way(osmid_t id, int exists) {
    return 0;
}

void output_mysql_t::enqueue_relations(pending_queue_t &job_queue, osmid_t id, size_t output_id, size_t& added) {
}

int output_mysql_t::pending_relation(osmid_t id, int exists) {
    return 0;
}

int output_mysql_t::node_add(osmid_t, double, double, const taglist_t &) {
  return 0;
}

int output_mysql_t::way_add(osmid_t a, const idlist_t &, const taglist_t &) {
  return 0;
}

int output_mysql_t::relation_add(osmid_t a, const memberlist_t &, const taglist_t &) {
  return 0;
}

int output_mysql_t::node_delete(osmid_t i) {
  return 0;
}

int output_mysql_t::way_delete(osmid_t i) {
  return 0;
}

int output_mysql_t::relation_delete(osmid_t i) {
  return 0;
}

int output_mysql_t::node_modify(osmid_t, double, double, const taglist_t &) {
  return 0;
}

int output_mysql_t::way_modify(osmid_t, const idlist_t &, const taglist_t &) {
  return 0;
}

int output_mysql_t::relation_modify(osmid_t, const memberlist_t &, const taglist_t &) {
  return 0;
}

std::shared_ptr<output_t> output_mysql_t::clone(const middle_query_t* cloned_middle) const {
    output_mysql_t *clone = new output_mysql_t(*this);
    clone->m_mid = cloned_middle;
    return std::shared_ptr<output_t>(clone);
}

output_mysql_t::output_mysql_t(const middle_query_t* mid_, const options_t &options_): output_t(mid_, options_) {
}

output_mysql_t::output_mysql_t(const output_mysql_t& other): output_t(other.m_mid, other.m_options) {
}

output_mysql_t::~output_mysql_t() {
}

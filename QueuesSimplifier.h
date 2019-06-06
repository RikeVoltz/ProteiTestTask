#pragma once

#include <boost/regex.hpp>
#include <boost/date_time.hpp>
#include <boost/algorithm/string_regex.hpp>
#include <unordered_set>
#include <unordered_map>

struct table {
    std::unordered_map<std::string, std::vector<std::string>> table;
    std::unordered_set<std::string> columns;
    size_t size;
};

struct database {
    boost::posix_time::ptime last_updated_time;
    std::unordered_map<std::string, table> database;
    std::set<std::pair<boost::posix_time::ptime, std::string>> insert_times;
    size_t queries_grouped;
};


class QueuesSimplifier {
private:
    std::unordered_map<std::string, database> _databases;
    std::ifstream _log;
    std::ofstream _result;


    const boost::regex _db_regex = boost::regex(
            R"((?i)(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(\w+)\s+(.+))"
    );

    const boost::regex _query_regex = boost::regex(
            R"((?i)(?:(INSERT(?: INTO)?|UPDATE)\s+(.+?)\s*(?:(SET|SELECT)\s+(.+?)|(?:VALUES)\s*\((.+?)\))(?:$|\;)\s*))"
    );
    const boost::regex _table_regex = boost::regex(R"((\w+)\s*\((.+)\))");

    void _outputGroupedQueries();

    void _createColumnIfNeeded(const std::string &db, const std::string &table, const std::string &column);

    void _parseTupleValues(const boost::posix_time::ptime &datetime, const std::string &db, std::string &table,
                           boost::smatch &queryRegexResult);

    void _fillEmptyValues(const std::string &db, const std::string &table);

    void _parseMappedValues(const std::string &db, std::string &table, boost::smatch &queryRegexResult);

    void _flushIfNeeded(boost::posix_time::ptime datetime, const std::string &db, const std::string &command);

public:
    explicit QueuesSimplifier(const std::string &log_path="queue.log", const std::string &result_path="result.log");

    void parse();

    ~QueuesSimplifier();
};

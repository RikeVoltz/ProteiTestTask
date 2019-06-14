#include "QueuesSimplifier.h"

std::string _datetime_to_str(const boost::posix_time::ptime &datetime) {
    boost::posix_time::time_facet *facet = new boost::posix_time::time_facet("%Y-%m-%d %H:%M:%S");
    std::ostringstream time;
    time.imbue(std::locale(std::locale::classic(), facet));
    time << datetime;
    return time.str();
}

void QueuesSimplifier::_outputGroupedQueries() {
    for (const auto &db_item:_databases) {
        for (const auto &insert_time:db_item.second.insert_times) {
            _result << _datetime_to_str(insert_time.first) << " " << db_item.first << " INSERT INTO "
                    << insert_time.second << "("
                    << boost::algorithm::join(db_item.second.tables.at(insert_time.second).column_names, ", ")
                    << ") VALUES ";
            for (int i = 0; i < db_item.second.tables.at(insert_time.second).size; i++) {
                if (i != 0)
                    _result << ", ";
                _result << "(";

                for (const auto &column:db_item.second.tables.at(insert_time.second).column_names) {
                    if (column != *db_item.second.tables.at(insert_time.second).column_names.begin())
                        _result << ", ";
                    _result << db_item.second.tables.at(insert_time.second).columns.at(column)[i];
                }
                _result << ")";
            }
            _result << ";" << std::endl;
        }
    }
}

void
QueuesSimplifier::_parseTupleValues(const boost::posix_time::ptime &datetime, const std::string &db, std::string &table,
                                    boost::smatch &queryRegexResult) {
    boost::smatch tableRegexResult;
    std::string table_string = queryRegexResult[2];
    boost::regex_match(table_string, tableRegexResult, _table_regex);
    table = tableRegexResult[1];
    std::string mapping = tableRegexResult[2];
    std::vector<std::string> column_names;
    boost::algorithm::split_regex(column_names, mapping, boost::regex(",\\s?"));
    std::vector<std::string> values;
    std::string values_string = queryRegexResult[4].length() ? queryRegexResult[4] : queryRegexResult[5];
    boost::algorithm::split_regex(values, values_string, boost::regex(",\\s?"));
    auto &current_database = _databases[db];
    if (!current_database.tables.count(table))
        current_database.insert_times.insert({datetime, table});
    auto &current_table = current_database.tables[table];
    for (int idx = 0; idx < column_names.size(); idx++) {
        _createColumnIfNeeded(db, table, column_names[idx]);
        current_table.columns[column_names[idx]].push_back(values[idx]);
    }
    current_table.size++;
    _fillEmptyValues(db, table);
    current_database.queries_grouped++;
}

void
QueuesSimplifier::_createColumnIfNeeded(const std::string &db, const std::string &table, const std::string &column_name) {
    auto &current_table = _databases[db].tables[table];
    if (!current_table.column_names.count(column_name)) {
        current_table.column_names.insert(column_name);
        current_table.columns[column_name].assign(current_table.size, "null");
    }
}

void QueuesSimplifier::_fillEmptyValues(const std::string &db, const std::string &table) {
    auto &current_table = _databases[db].tables[table];
    for (const auto &column_name:current_table.column_names)
        if (current_table.columns[column_name].size() <
            current_table.size)
            current_table.columns[column_name].push_back("null");
}

void QueuesSimplifier::_parseMappedValues(const std::string &db, std::string &table, boost::smatch &queryRegexResult) {
    table = queryRegexResult[2];
    std::string mapped_values = queryRegexResult[4];
    std::vector<std::string> columns_values;
    boost::algorithm::split_regex(columns_values, mapped_values, boost::regex(",\\s?"));
    for (const auto &column_value:columns_values) {
        std::vector<std::string> value_pair;
        boost::split(value_pair, column_value, boost::is_any_of("="));
        _createColumnIfNeeded(db, table, value_pair[0]);
        _databases[db].tables[table].columns[value_pair[0]].push_back(value_pair[1]);
    }
    _databases[db].tables[table].size++;
    _fillEmptyValues(db, table);
    _databases[db].queries_grouped++;
}

void
QueuesSimplifier::_flushIfNeeded(boost::posix_time::ptime datetime, const std::string &db, const std::string &command) {
    if (_databases.count(db)) {
        auto &current_database = _databases[db];
        if (datetime - current_database.last_updated_time > boost::posix_time::minutes(1) ||
            current_database.queries_grouped == 10 || command == "UPDATE" || command.empty()) {
            _outputGroupedQueries();
            current_database.insert_times.clear();
            current_database.tables.clear();
            current_database.queries_grouped = 0;
            current_database.last_updated_time = datetime;
        }
    }
}

QueuesSimplifier::QueuesSimplifier(const std::string &log_path, const std::string &result_path) {
    _log.open(log_path, std::fstream::in);
    _result.open(result_path, std::fstream::out);
}

void QueuesSimplifier::parse() {
    std::string record;
    boost::smatch dbRegexResult;
    boost::smatch queryRegexResult;
    while (getline(_log, record)) {
        boost::regex_match(record, dbRegexResult, _db_regex);
        auto datetime = boost::date_time::parse_delimited_time<boost::posix_time::ptime>(dbRegexResult[1], ' ');
        std::string db = dbRegexResult[2];
        std::string query = dbRegexResult[3];
        boost::regex_match(query, queryRegexResult, _query_regex);
        std::string command = queryRegexResult[1];
        std::string table;
        _flushIfNeeded(datetime, db, command);
        if (boost::to_upper_copy(command.substr(0, 6)) == "INSERT") {
            if (boost::to_upper_copy(queryRegexResult.str(3)) != "SET")
                _parseTupleValues(datetime, db, table, queryRegexResult);
            else
                _parseMappedValues(db, table, queryRegexResult);
        } else {
            _result << dbRegexResult[0] << std::endl;
        }
    }
    _outputGroupedQueries();
}

QueuesSimplifier::~QueuesSimplifier() {
    _log.close();
    _result.close();
}

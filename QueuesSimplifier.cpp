#include "QueuesSimplifier.h"

std::string _datetime_to_str(const boost::posix_time::ptime &datetime) {
    boost::posix_time::time_facet *facet = new boost::posix_time::time_facet("%Y-%m-%d %H:%M:%S");
    std::ostringstream time;
    time.imbue(std::locale(std::locale::classic(), facet));
    time << datetime;
    return time.str();
}

void QueuesSimplifier::_outputGroupedQueries() {
    for (const auto &db_item:this->_databases) {
        for (const auto &insert_time:db_item.second.insert_times) {
            _result << _datetime_to_str(insert_time.first) << " " << db_item.first << " INSERT INTO "
                    << insert_time.second << "("
                    << boost::algorithm::join(db_item.second.database.at(insert_time.second).columns, ", ")
                    << ") VALUES ";
            for (int i = 0; i < db_item.second.database.at(insert_time.second).size; i++) {
                if (i != 0)
                    _result << ", ";
                _result << "(";

                for (const auto &column:db_item.second.database.at(insert_time.second).columns) {
                    if (column != *db_item.second.database.at(insert_time.second).columns.begin())
                        _result << ", ";
                    _result << db_item.second.database.at(insert_time.second).table.at(column)[i];
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
    std::vector<std::string> columns;
    boost::algorithm::split_regex(columns, mapping, boost::regex(",\\s?"));
    std::vector<std::string> values;
    std::string values_string = queryRegexResult[4].length() ? queryRegexResult[4] : queryRegexResult[5];
    boost::algorithm::split_regex(values, values_string, boost::regex(",\\s?"));
    if (!this->_databases[db].database.count(table))
        this->_databases[db].insert_times.insert({datetime, table});
    for (int idx = 0; idx < columns.size(); idx++) {
        this->_createColumnIfNeeded(db, table, columns[idx]);
        this->_databases[db].database[table].table[columns[idx]].push_back(values[idx]);
    }
    this->_databases[db].database[table].size++;
    this->_fillEmptyValues(db, table);
    this->_databases[db].queries_grouped++;
}

void
QueuesSimplifier::_createColumnIfNeeded(const std::string &db, const std::string &table, const std::string &column) {
    if (!this->_databases[db].database[table].columns.count(column)) {
        this->_databases[db].database[table].columns.insert(column);
        this->_databases[db].database[table].table[column].assign(this->_databases[db].database[table].size,
                                                                  "null");
    }
}

void QueuesSimplifier::_fillEmptyValues(const std::string &db, const std::string &table) {
    for (const auto &column:this->_databases[db].database[table].columns)
        if (this->_databases[db].database[table].table[column].size() <
            this->_databases[db].database[table].size)
            this->_databases[db].database[table].table[column].push_back("null");
}

void QueuesSimplifier::_parseMappedValues(const std::string &db, std::string &table, boost::smatch &queryRegexResult) {
    table = queryRegexResult[2];
    std::string mapped_values = queryRegexResult[4];
    std::vector<std::string> columns_values;
    boost::algorithm::split_regex(columns_values, mapped_values, boost::regex(",\\s?"));
    for (const auto &column_value:columns_values) {
        std::vector<std::string> value_pair;
        boost::split(value_pair, column_value, boost::is_any_of("="));
        this->_createColumnIfNeeded(db, table, value_pair[0]);
        this->_databases[db].database[table].table[value_pair[0]].push_back(value_pair[1]);
    }
    this->_databases[db].database[table].size++;
    this->_fillEmptyValues(db, table);
    this->_databases[db].queries_grouped++;
}

void
QueuesSimplifier::_flushIfNeeded(boost::posix_time::ptime datetime, const std::string &db, const std::string &command) {
    if (this->_databases.count(db)) {
        if (datetime - this->_databases[db].last_updated_time > boost::posix_time::minutes(1) ||
            this->_databases[db].queries_grouped == 10 || command == "UPDATE" || command.empty()) {
            this->_outputGroupedQueries();
            this->_databases[db].insert_times.clear();
            this->_databases[db].database.clear();
            this->_databases[db].queries_grouped = 0;
            this->_databases[db].last_updated_time = datetime;
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
        this->_flushIfNeeded(datetime, db, command);
        if (boost::to_upper_copy(command.substr(0, 6)) == "INSERT") {
            if (boost::to_upper_copy(queryRegexResult.str(3)) != "SET")
                this->_parseTupleValues(datetime, db, table, queryRegexResult);
            else
                this->_parseMappedValues(db, table, queryRegexResult);
        } else {
            _result << dbRegexResult[0] << std::endl;
        }
    }
    this->_outputGroupedQueries();
}

QueuesSimplifier::~QueuesSimplifier() {
    _log.close();
    _result.close();
}

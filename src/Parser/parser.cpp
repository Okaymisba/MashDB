#include "parser.h"
#include "../Creation/createDatabase.h"
#include "../Operations/ChangeDB/changeDB.h"
#include "../Operations/Insertion/insert.h"

#include <regex>
#include <iostream>
#include <algorithm>
#include <sstream>

/**
 * Parses a given SQL-like query and performs the corresponding action based on the query type.
 * Supports INSERT INTO, CREATE DATABASE, and CHANGE DATABASE queries. Throws an exception
 * if the query is invalid or not supported. The method also handles empty queries by throwing
 * a runtime exception.
 *
 * @param query A constant reference to the SQL-like query string to be parsed and executed.
 *              The query must follow the expected format:
 *              - For INSERT: INSERT INTO <table_name>(<columns>) VALUES (<values>);
 *              - For CREATE DATABASE: CREATE DATABASE <database_name>;
 *              - For CHANGE DATABASE: CHANGE DATABASE <database_name>;
 *              If the query does not match any of these formats, a runtime_error is thrown.
 *
 * @throws std::runtime_error Thrown when the query is invalid, empty, or does not match
 *                            the expected format.
 */
void ParseQuery::parse(const std::string &query) {
    if (query.empty()) {
        throw std::runtime_error("Empty query");
    }

    std::regex insertRegex(
        R"(^\s*INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_$]*)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)\s*;$)",
        std::regex_constants::icase
    );

    std::regex createDbRegex(
        R"(^\s*CREATE\s+DATABASE\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*;$)",
        std::regex_constants::icase
    );

    std::regex changeDbRegex(
        R"(^\s*CHANGE\s+DATABASE\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*;$)",
        std::regex_constants::icase
    );

    std::smatch match;

    if (std::regex_match(query, match, insertRegex)) {
        std::string tableName = match[1].str();
        std::string columnsStr = match[2].str();
        std::string valuesStr = match[3].str();

        std::vector<std::string> columns;
        std::stringstream colStream(columnsStr);
        std::string col;
        while (std::getline(colStream, col, ',')) {
            col.erase(remove_if(col.begin(), col.end(), ::isspace), col.end());
            columns.push_back(col);
        }

        std::vector<std::string> values;
        std::stringstream valStream(valuesStr);
        std::string val;
        while (std::getline(valStream, val, ',')) {
            val.erase(remove_if(val.begin(), val.end(), ::isspace), val.end());
            if (!val.empty() && (val.front() == '\'' || val.front() == '"')) {
                val.erase(0, 1);
            }
            if (!val.empty() && (val.back() == '\'' || val.back() == '"')) {
                val.pop_back();
            }
            values.push_back(val);
        }

        InsertIntoTable::insert("testDatabase29Dec", tableName, columns, values);
        std::cout << "Record inserted successfully into table: " << tableName << std::endl;
    } else if (std::regex_match(query, match, createDbRegex)) {
        std::string dbName = match[1].str();
        CreateDatabase::createDatabase(dbName);
        std::cout << "Database created: " << dbName << std::endl;
    } else if (std::regex_match(query, match, changeDbRegex)) {
        std::string dbName = match[1].str();
        ChangeDB::change(dbName);
        std::cout << "Changed current database to: " << dbName << std::endl;
    } else {
        throw std::runtime_error("Invalid query: " + query);
    }
}

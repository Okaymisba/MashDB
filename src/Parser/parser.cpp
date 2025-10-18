#include "parser.h"
#include "../Creation/createDatabase.h"
#include "../Operations/ChangeDB/changeDB.h"
#include "../Operations/Insertion/insert.h"
#include "../Operations/CurrentDB/currentDB.h"

#include <regex>
#include <iostream>
#include <sstream>

using namespace std;

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
void ParseQuery::parse(const string &query) {
    if (query.empty()) {
        throw runtime_error("Empty query");
    }

    regex insertRegex(
        R"(^\s*INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_$]*)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)\s*;$)",
        regex_constants::icase
    );

    regex createDbRegex(
        R"(^\s*CREATE\s+DATABASE\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*;$)",
        regex_constants::icase
    );

    regex changeDbRegex(
        R"(^\s*CHANGE\s+DATABASE\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*;$)",
        regex_constants::icase
    );

    smatch match;

    if (regex_match(query, match, insertRegex)) {
        string tableName = match[1].str();
        string columnsStr = match[2].str();
        string valuesStr = match[3].str();

        // Parse columns
        vector<string> columns;
        stringstream ssCol(columnsStr);
        string col;
        while (getline(ssCol, col, ',')) {
            // Trim whitespace
            col.erase(0, col.find_first_not_of(" \t\n\r\f\v"));
            col.erase(col.find_last_not_of(" \t\n\r\f\v") + 1);
            if (!col.empty()) {
                columns.push_back(col);
            }
        }

        // Parse values
        vector<string> values;
        stringstream ssVal(valuesStr);
        string val;
        while (getline(ssVal, val, ',')) {
            // Trim whitespace and quotes
            val.erase(0, val.find_first_not_of(" \t\n\r\f\v\'\""));
            val.erase(val.find_last_not_of(" \t\n\r\f\v\'\"") + 1);
            if (!val.empty()) {
                values.push_back(val);
            }
        }

        // Call insert function
        InsertIntoTable::insert(CurrentDB::get(), tableName, columns, values);
    } else if (regex_match(query, match, createDbRegex)) {
        string dbName = match[1].str();
        CreateDatabase::createDatabase(dbName);
    } else if (regex_match(query, match, changeDbRegex)) {
        string dbName = match[1].str();
        ChangeDB::change(dbName);
    } else {
        throw runtime_error("Invalid query");
    }
}

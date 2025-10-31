#include "parser.h"
#include "../Creation/createDatabase.h"
#include "../Operations/ChangeDB/changeDB.h"
#include "../Operations/Insertion/insert.h"
#include "../Operations/Selection/select.h"
#include "../Operations/CurrentDB/currentDB.h"

#include <regex>
#include <iostream>
#include <sstream>

using namespace std;

/**
 * Parses and executes SQL-like database queries. Validates query syntax and dispatches
 * to the appropriate operation handler. Supports a subset of SQL operations for database
 * management and data manipulation.
 *
 * Supported Operations:
 * - INSERT INTO table_name(column1, column2) VALUES (value1, value2);
 *   Inserts a new record into the specified table with the given values.
 *
 * - CREATE DATABASE database_name;
 *   Creates a new database with the specified name.
 *
 * - CHANGE DATABASE database_name;
 *   Changes the current working database context.
 *
 * @param query The SQL query string to parse and execute
 * @throws std::runtime_error If the query is empty, malformed, or contains unsupported syntax
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

    // SELECT statement patterns
    regex selectBasicRegex(
        R"(^\s*SELECT\s+(\*|(?:\s*[a-zA-Z_][a-zA-Z0-9_]*(?:\s*,\s*[a-zA-Z_][a-zA-Z0-9_]*)*))\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)(?:\s*;)?\s*$)",
        regex_constants::icase
    );

    regex selectWhereRegex(
        R"(^\s*SELECT\s+(\*|(?:\s*[a-zA-Z_][a-zA-Z0-9_]*(?:\s*,\s*[a-zA-Z_][a-zA-Z0-9_]*)*))\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+WHERE\s+(.+?)(?:\s*;)?\s*$)",
        regex_constants::icase
    );

    regex selectOrderByRegex(
        R"(^\s*SELECT\s+(\*|(?:\s*[a-zA-Z_][a-zA-Z0-9_]*(?:\s*,\s*[a-zA-Z_][a-zA-Z0-9_]*)*))\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+ORDER BY\s+([a-zA-Z_][a-zA-Z0-9_]*)(?:\s+(ASC|DESC))?(?:\s*;)?\s*$)",
        regex_constants::icase
    );

    regex selectLimitRegex(
        R"(^\s*SELECT\s+(\*|(?:\s*[a-zA-Z_][a-zA-Z0-9_]*(?:\s*,\s*[a-zA-Z_][a-zA-Z0-9_]*)*))\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)(?:\s+LIMIT\s+(\d+)(?:\s+OFFSET\s+(\d+))?)?(?:\s*;)?\s*$)",
        regex_constants::icase
    );

    regex selectFullRegex(
        R"(^\s*SELECT\s+(\*|(?:\s*[a-zA-Z_][a-zA-Z0-9_]*(?:\s*,\s*[a-zA-Z_][a-zA-Z0-9_]*)*))\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER BY\s+([a-zA-Z_][a-zA-Z0-9_]*)(?:\s+(ASC|DESC))?)?(?:\s+LIMIT\s+(\d+)(?:\s+OFFSET\s+(\d+))?)?(?:\s*;)?\s*$)",
        regex_constants::icase
    );

    smatch match;

    if (regex_match(query, match, insertRegex)) {
        string tableName = match[1].str();
        string columnsStr = match[2].str();
        string valuesStr = match[3].str();

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

        vector<json> values;
        stringstream ssVal(valuesStr);
        string val;

        auto trim = [](string s) -> string {
            s.erase(0, s.find_first_not_of(" \t\n\r\f\v"));
            s.erase(s.find_last_not_of(" \t\n\r\f\v") + 1);
            return s;
        };

        while (getline(ssVal, val, ',')) {
            val = trim(val);
            if (val.empty()) continue; // Skip empty values

            if (val == "NULL" || val == "null") {
                // NULL values
                values.emplace_back(nullptr);
            }
            // Handle true/false (case doesn't matter)
            else if (val == "true" || val == "TRUE") {
                values.emplace_back(true);
            } else if (val == "false" || val == "FALSE") {
                values.emplace_back(false);
            }
            // Remove quotes from strings
            else if ((val[0] == '\'' && val.back() == '\'') ||
                     (val[0] == '"' && val.back() == '"')) {
                values.emplace_back(val.substr(1, val.length() - 2));
            }
            // Check for whole numbers
            else if (regex_match(val, regex("^-?\\d+$"))) {
                values.emplace_back(stoi(val));
            } else if (regex_match(val, regex(R"(^-?\d+\.\d+$)"))) {
                values.emplace_back(stod(val));
            }
            // If nothing else matches, treat as plain string
            else {
                values.emplace_back(val);
            }
        }

        InsertIntoTable::insert(CurrentDB::getCurrentDB(), tableName, columns, values);
    } else if (regex_match(query, match, selectBasicRegex) ||
               regex_match(query, match, selectWhereRegex) ||
               regex_match(query, match, selectOrderByRegex) ||
               regex_match(query, match, selectLimitRegex) ||
               regex_match(query, match, selectFullRegex)) {
        string columnsStr = match[1].str();
        string tableName = match[2].str();

        // Parse columns
        vector<string> columns;
        if (columnsStr != "*") {
            stringstream ss(columnsStr);
            string col;
            while (getline(ss, col, ',')) {
                // Trim whitespace
                col.erase(0, col.find_first_not_of(" \t\n\r\f\v"));
                col.erase(col.find_last_not_of(" \t\n\r\f\v") + 1);
                if (!col.empty()) {
                    columns.push_back(col);
                }
            }
        }

        // Default values
        string whereConditionStr;
        string orderByColumn;
        bool ascending = true;
        optional<size_t> limit = nullopt;
        size_t offset = 0;

        // Parse full query if matched by the full regex
        if (match.size() > 3) {
            // WHERE clause
            if (match[3].matched) {
                whereConditionStr = match[3].str();
            }

            // ORDER BY
            if (match[4].matched) {
                orderByColumn = match[4].str();
                if (match[5].matched) {
                    string order = match[5].str();
                    ascending = (order != "DESC" && order != "desc");
                }
            }

            // LIMIT and OFFSET
            if (match[6].matched) {
                limit = stoul(match[6].str());
                if (match[7].matched) {
                    offset = stoul(match[7].str());
                }
            }
        }

        // Convert WHERE condition string to function (simplified for this example)
        function<bool(const json &)> whereCondition = nullptr;
        if (!whereConditionStr.empty()) {
            // In a real implementation, you would parse the whereConditionStr
            // and create a proper function that evaluates the condition
            // For now, we'll just create a placeholder
            whereCondition = [](const json &) { return true; };
        }

        // Execute the query
        json result = Selection::selectFromTable(
            CurrentDB::getCurrentDB(),
            tableName,
            columns,
            whereCondition,
            orderByColumn,
            ascending,
            limit,
            offset
        );

        // Print the result (in a real implementation, you might want to format this better)
        cout << result.dump(4) << endl;
    } else if (regex_match(query, match, createDbRegex)) {
        string dbName = match[1].str();
        CreateDatabase::createDatabase(dbName);
    } else if (regex_match(query, match, changeDbRegex)) {
        string dbName = match[1].str();
        ChangeDB::change(dbName);
        throw runtime_error("Invalid query");
    }
}

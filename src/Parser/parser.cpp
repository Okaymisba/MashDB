#include "parser.h"

#include <fstream>

#include "../Operations/Creation/createDatabase.h"
#include "../Operations/ChangeDB/changeDB.h"
#include "../Operations/Insertion/insert.h"
#include "../Operations/Selection/select.h"
#include "../Operations/Selection/ResultFormatter.hpp"
#include "../Operations/CurrentDB/currentDB.h"
#include "../Operations/Creation/createTable.h"
#include "../Operations/Deletion/deleteRow.h"
#include "../Operations/Update/updateRow.h"

#include <regex>
#include <iostream>
#include <sstream>

#include "conditionParser.h"

using namespace std;
namespace fs = filesystem;

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

    regex createTableRegex(
        R"(^\s*CREATE\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_$]*)\s*\((.+)\)\s*;\s*$)",
        regex_constants::icase
    );

    // DELETE FROM table_name WHERE condition;
    regex deleteRegex(
        R"(^\s*DELETE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_$]*)(?:\s+WHERE\s+(.+?))?\s*;\s*$)",
        regex_constants::icase
    );

    // UPDATE table_name SET column1=value1, column2=value2 WHERE condition;
    regex updateRegex(
        R"(^\s*UPDATE\s+([a-zA-Z_][a-zA-Z0-9_$]*)\s+SET\s+([^;]+?)(?:\s+WHERE\s+(.+?))?\s*;\s*$)",
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
    } else if (regex_match(query, match, selectFullRegex)) {
        string columnsStr = match[1].str();
        string tableName = match[2].str();

        // Parse columns
        vector<string> columns;
        if (columnsStr != "*") {
            stringstream ss(columnsStr);
            string col;
            while (getline(ss, col, ',')) {
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

        // Parse WHERE condition if provided
        function<bool(const json &)> whereCondition = nullptr;
        if (!whereConditionStr.empty()) {
            try {
                // Parse the condition string
                Condition condition = ConditionParser::parseCondition(whereConditionStr);

                // Get the table info to validate the column exists
                string basePath = fs::current_path().string() + "/Databases/" + CurrentDB::getCurrentDB() + "/" +
                                  tableName;
                fs::path tableInfoFile = basePath + "/Table-info.json";

                if (!fs::exists(tableInfoFile)) {
                    throw runtime_error("Table info not found");
                }

                json tableInfo;
                {
                    ifstream tfile(tableInfoFile);
                    if (!tfile) throw runtime_error("Failed to open table info file");
                    tfile >> tableInfo;
                }

                // Check if the condition column exists in the table
                bool columnExists = false;
                for (auto it = tableInfo.begin(); it != tableInfo.end(); ++it) {
                    if (strcasecmp(it.key().c_str(), condition.column.c_str()) == 0) {
                        condition.column = it.key(); // Use the exact column name from the table
                        columnExists = true;
                        break;
                    }
                }

                if (!columnExists) {
                    throw runtime_error("Column not found in table: " + condition.column);
                }

                // Create a lambda that evaluates the condition for a given row
                whereCondition = [condition](const json &row) -> bool {
                    try {
                        if (!row.contains(condition.column)) {
                            throw runtime_error("Column '" + condition.column + "' not found in row");
                        }

                        // Evaluate the condition directly using the JSON value
                        return ConditionParser::evaluateCondition(row[condition.column], condition);
                    } catch (const exception &e) {
                        cerr << "Error evaluating condition: " << e.what() << endl;
                        return false;
                    }
                };
            } catch (const exception &e) {
                throw runtime_error("Invalid WHERE condition: " + string(e.what()));
            }
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

        // Format and print the result as a table
        vector<string> selectedCols;
        if (columnsStr != "*") {
            selectedCols = columns;
        }
        cout << Selection::ResultFormatter::formatAsTable(result, selectedCols);
    } else if (regex_match(query, match, deleteRegex)) {
        string tableName = match[1].str();
        string condition = match[2].matched ? match[2].str() : "";

        if (condition.empty()) {
            throw runtime_error("DELETE without WHERE clause is not supported for safety");
        }

        string normalizedCondition = condition;

        size_t pos = 0;
        while ((pos = normalizedCondition.find("==", pos)) != string::npos) {
            normalizedCondition.replace(pos, 2, "=");
            pos += 1;
        }

        regex ws_re("\\s*([=!<>]+)\\s*");
        normalizedCondition = regex_replace(normalizedCondition, ws_re, " $1 ");

        auto trim = [](string s) -> string {
            s.erase(0, s.find_first_not_of(" \t\n\r\f\v"));
            s.erase(s.find_last_not_of(" \t\n\r\f\v") + 1);
            return s;
        };

        string formattedCondition = trim(normalizedCondition);

        DeleteRow::deleteRow(tableName, formattedCondition);
    } else if (regex_match(query, match, createTableRegex)) {
        string tableName = match[1].str();
        string defsStr = match[2].str();

        // Split column definitions by comma
        vector<string> rawDefs;
        string part;
        stringstream ssDefs(defsStr);
        while (getline(ssDefs, part, ',')) {
            // trim
            part.erase(part.find_last_not_of(" \t\n\r\f\v") + 1);
            if (!part.empty()) rawDefs.push_back(part);
        }

        vector<string> columns;
        vector<string> dataTypes;
        vector<bool> isUnique;
        vector<bool> notNull;

        for (auto &def: rawDefs) {
            // Tokenize by spaces to find name, type and constraints
            stringstream ts(def);
            string name;
            if (!(ts >> name)) continue;

            // Read the remainder of the definition line
            string rest;
            getline(ts, rest);
            // trim
            rest.erase(0, rest.find_first_not_of(" \t\n\r\f\v"));
            rest.erase(rest.find_last_not_of(" \t\n\r\f\v") + 1);

            // Determine constraints
            bool uniqueFlag = false;
            bool notNullFlag = false;

            // For robust parsing, search case-insensitively
            string restLower = rest;
            for (auto &ch: restLower) ch = static_cast<char>(tolower(ch));

            if (restLower.find("unique") != string::npos) uniqueFlag = true;
            if (restLower.find("not null") != string::npos) {
                notNullFlag = true;
            } else if (restLower.find(" null") != string::npos) {
                // Explicit NULL resets not-null
                notNullFlag = false;
            }

            string typePart = rest;
            // Normalize spaces for easier removal
            // Remove 'UNIQUE' and 'NOT NULL' (any casing)
            {
                string tmp = typePart;
                // To remove case-insensitively, iterate through possibilities
                // First remove NOT NULL then UNIQUE
                // Simple approach: build lowercase copies for search, but erase from original indexes is complex.
                // Instead, replace occurrences by scanning tokens.
                string out;
                string token;
                stringstream rts(typePart);
                while (rts >> token) {
                    string tl = token;
                    for (auto &c: tl) c = static_cast<char>(tolower(c));
                    if (tl == "unique") {
                        continue; // drop
                    }
                    if (tl == "not") {
                        // peek next
                        streampos p = rts.tellg();
                        string nxt;
                        if (rts >> nxt) {
                            string nl = nxt;
                            for (auto &c: nl) c = static_cast<char>(tolower(c));
                            if (nl == "null") {
                                // drop both
                                continue;
                            } else {
                                // restore position if not 'NULL'
                                rts.seekg(p);
                            }
                        }
                        // keep 'not' if not followed by NULL
                        if (!out.empty()) out.push_back(' ');
                        out += token;
                    } else {
                        if (!out.empty()) out.push_back(' ');
                        out += token;
                    }
                }
                typePart = out;
            }

            // trim typePart
            typePart.erase(0, typePart.find_first_not_of(" \t\n\r\f\v"));
            typePart.erase(typePart.find_last_not_of(" \t\n\r\f\v") + 1);
            if (typePart.empty()) {
                // Fallback: if no explicit type remains, mark as TEXT
                typePart = "TEXT";
            }

            columns.push_back(name);
            dataTypes.push_back(typePart);
            isUnique.push_back(uniqueFlag);
            notNull.push_back(notNullFlag);
        }

        CreateTable::createTable(tableName, columns, dataTypes, isUnique, notNull);
    } else if (regex_match(query, match, createDbRegex)) {
        string dbName = match[1].str();
        CreateDatabase::createDatabase(dbName);
    } else if (regex_match(query, match, updateRegex)) {
        string tableName = match[1].str();
        string setClause = match[2].str();
        string whereClause = match[3].matched ? match[3].str() : "";

        // Parse SET clause
        unordered_map<string, json> updates;
        stringstream ssSet(setClause);
        string setItem;

        while (getline(ssSet, setItem, ',')) {
            // Trim whitespace
            setItem.erase(0, setItem.find_first_not_of(" \t\n\r\f\v"));
            setItem.erase(setItem.find_last_not_of(" \t\n\r\f\v") + 1);

            size_t eqPos = setItem.find('=');
            if (eqPos == string::npos) {
                throw runtime_error("Invalid SET clause: " + setItem);
            }

            string column = setItem.substr(0, eqPos);
            column.erase(0, column.find_first_not_of(" \t\n\r\f\v"));
            column.erase(column.find_last_not_of(" \t\n\r\f\v") + 1);

            string valueStr = setItem.substr(eqPos + 1);
            valueStr.erase(0, valueStr.find_first_not_of(" \t\n\r\f\v"));
            valueStr.erase(valueStr.find_last_not_of(" \t\n\r\f\v") + 1);

            // Parse the value
            json value;
            if (valueStr == "NULL" || valueStr == "null") {
                value = nullptr;
            } else if (valueStr == "true" || valueStr == "TRUE") {
                value = true;
            } else if (valueStr == "false" || valueStr == "FALSE") {
                value = false;
            }
            // Handle quoted strings
            else if ((valueStr[0] == '\'' && valueStr.back() == '\'') ||
                     (valueStr[0] == '"' && valueStr.back() == '"')) {
                value = valueStr.substr(1, valueStr.length() - 2);
            }
            // Handle numbers
            else if (regex_match(valueStr, regex("^-?\\d+$"))) {
                value = stoi(valueStr);
            } else if (regex_match(valueStr, regex(R"(^-?\d+\.\d+$)"))) {
                value = stod(valueStr);
            } else {
                // Default to string if no other type matches
                value = valueStr;
            }

            updates[column] = value;
        }

        // Normalize the WHERE clause if it exists
        string normalizedWhere = whereClause;
        if (!whereClause.empty()) {
            // Replace == with = for consistency
            size_t pos = 0;
            while ((pos = normalizedWhere.find("==", pos)) != string::npos) {
                normalizedWhere.replace(pos, 2, "=");
                pos += 1;
            }

            // Normalize spaces around operators
            regex ws_re("\\s*([=!<>]+)\\s*");
            normalizedWhere = regex_replace(normalizedWhere, ws_re, " $1 ");

            // Trim the result
            normalizedWhere.erase(0, normalizedWhere.find_first_not_of(" \t\n\r\f\v"));
            normalizedWhere.erase(normalizedWhere.find_last_not_of(" \t\n\r\f\v") + 1);
        }

        // Call the update function
        int updated = UpdateOperation::updateTable(
            tableName,
            updates,
            normalizedWhere
        );

        if (updated < 0) {
            throw runtime_error("Failed to update rows in table " + tableName);
        }
    } else if (regex_match(query, match, changeDbRegex)) {
        string dbName = match[1].str();
        ChangeDB::change(dbName);
    }
}

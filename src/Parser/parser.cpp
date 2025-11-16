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
 * Parse a SQL query and execute the corresponding the appropriate operation.
 *
 * The following operations are supported:
 *   - INSERT INTO table_name (column1, column2, ...) VALUES (value1, value2, ...)
 *   - SELECT columns FROM table_name WHERE condition
 *   - DELETE FROM table_name WHERE condition
 *   - CREATE TABLE table_name (column1 type, column2 type, ...)
 *   - CREATE DATABASE database_name
 *   - CHANGE DATABASE database_name
 *   - UPDATE table_name SET column1=value1, column2=value2, ... WHERE condition
 *
 * @param query The SQL query to parse and execute.
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

    regex deleteRegex(
        R"(^\s*DELETE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_$]*)(?:\s+WHERE\s+(.+?))?\s*;\s*$)",
        regex_constants::icase
    );

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
                values.emplace_back(nullptr);
            } else if (val == "true" || val == "TRUE") {
                values.emplace_back(true);
            } else if (val == "false" || val == "FALSE") {
                values.emplace_back(false);
            } else if ((val[0] == '\'' && val.back() == '\'') ||
                       (val[0] == '"' && val.back() == '"')) {
                values.emplace_back(val.substr(1, val.length() - 2));
            } else if (regex_match(val, regex("^-?\\d+$"))) {
                values.emplace_back(stoi(val));
            } else if (regex_match(val, regex(R"(^-?\d+\.\d+$)"))) {
                values.emplace_back(stod(val));
            } else {
                values.emplace_back(val);
            }
        }

        InsertIntoTable::insert(CurrentDB::getCurrentDB(), tableName, columns, values);
    } else if (regex_match(query, match, selectFullRegex)) {
        string columnsStr = match[1].str();
        string tableName = match[2].str();

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

        string whereConditionStr;
        string orderByColumn;
        bool ascending = true;
        optional<size_t> limit = nullopt;
        size_t offset = 0;

        if (match.size() > 3) {
            if (match[3].matched) {
                whereConditionStr = match[3].str();
            }
            if (match[4].matched) {
                orderByColumn = match[4].str();
                if (match[5].matched) {
                    string order = match[5].str();
                    ascending = (order != "DESC" && order != "desc");
                }
            }
            if (match[6].matched) {
                limit = stoul(match[6].str());
                if (match[7].matched) {
                    offset = stoul(match[7].str());
                }
            }
        }

        function<bool(const json &)> whereCondition = nullptr;
        if (!whereConditionStr.empty()) {
            try {
                Condition condition = ConditionParser::parseCondition(whereConditionStr);

                fs::path homeDir = getenv("HOME");
                if (homeDir.empty()) homeDir = getenv("USERPROFILE");
                fs::path basePath = homeDir / ".mashdb" / "databases" / CurrentDB::getCurrentDB() / tableName;
                fs::path tableInfoFile = basePath / "Table-info.json";

                if (!fs::exists(tableInfoFile)) {
                    throw runtime_error("Table info not found");
                }

                json tableInfo;
                {
                    ifstream tfile(tableInfoFile);
                    if (!tfile) throw runtime_error("Failed to open table info file");
                    tfile >> tableInfo;
                }

                bool columnExists = false;
                for (auto it = tableInfo.begin(); it != tableInfo.end(); ++it) {
                    if (strcasecmp(it.key().c_str(), condition.column.c_str()) == 0) {
                        condition.column = it.key();
                        columnExists = true;
                        break;
                    }
                }

                if (!columnExists) {
                    throw runtime_error("Column not found in table: " + condition.column);
                }

                whereCondition = [condition](const json &row) -> bool {
                    try {
                        if (!row.contains(condition.column)) {
                            throw runtime_error("Column '" + condition.column + "' not found in row");
                        }

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

        vector<string> rawDefs;
        string part;
        stringstream ssDefs(defsStr);
        while (getline(ssDefs, part, ',')) {
            part.erase(part.find_last_not_of(" \t\n\r\f\v") + 1);
            if (!part.empty()) rawDefs.push_back(part);
        }

        vector<string> columns;
        vector<string> dataTypes;
        vector<bool> isUnique;
        vector<bool> notNull;

        for (auto &def: rawDefs) {
            stringstream ts(def);
            string name;
            if (!(ts >> name)) continue;
            string rest;
            getline(ts, rest);
            rest.erase(0, rest.find_first_not_of(" \t\n\r\f\v"));
            rest.erase(rest.find_last_not_of(" \t\n\r\f\v") + 1);

            bool uniqueFlag = false;
            bool notNullFlag = false;

            string restLower = rest;
            for (auto &ch: restLower) ch = static_cast<char>(tolower(ch));

            if (restLower.find("unique") != string::npos) uniqueFlag = true;
            if (restLower.find("not null") != string::npos) notNullFlag = true;
            else if (restLower.find(" null") != string::npos) notNullFlag = false;

            string typePart = rest;
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
                        continue;
                    }
                    if (tl == "not") {
                        streampos p = rts.tellg();
                        string nxt;
                        if (rts >> nxt) {
                            string nl = nxt;
                            for (auto &c: nl) c = static_cast<char>(tolower(c));
                            if (nl == "null") {
                                continue;
                            } else {
                                rts.seekg(p);
                            }
                        }
                        if (!out.empty()) out.push_back(' ');
                        out += token;
                    } else {
                        if (!out.empty()) out.push_back(' ');
                        out += token;
                    }
                }
                typePart = out;
            }

            typePart.erase(0, typePart.find_first_not_of(" \t\n\r\f\v"));
            typePart.erase(typePart.find_last_not_of(" \t\n\r\f\v") + 1);
            if (typePart.empty()) {
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

        unordered_map<string, json> updates;
        stringstream ssSet(setClause);
        string setItem;

        while (getline(ssSet, setItem, ',')) {
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

            json value;
            if (valueStr == "NULL" || valueStr == "null") {
                value = nullptr;
            } else if (valueStr == "true" || valueStr == "TRUE") {
                value = true;
            } else if (valueStr == "false" || valueStr == "FALSE") {
                value = false;
            } else if ((valueStr[0] == '\'' && valueStr.back() == '\'') ||
                       (valueStr[0] == '"' && valueStr.back() == '"')) {
                value = valueStr.substr(1, valueStr.length() - 2);
            } else if (regex_match(valueStr, regex("^-?\\d+$"))) {
                value = stoi(valueStr);
            } else if (regex_match(valueStr, regex(R"(^-?\d+\.\d+$)"))) {
                value = stod(valueStr);
            } else {
                value = valueStr;
            }

            updates[column] = value;
        }

        string normalizedWhere = whereClause;
        if (!whereClause.empty()) {
            size_t pos = 0;
            while ((pos = normalizedWhere.find("==", pos)) != string::npos) {
                normalizedWhere.replace(pos, 2, "=");
                pos += 1;
            }

            regex ws_re("\\s*([=!<>]+)\\s*");
            normalizedWhere = regex_replace(normalizedWhere, ws_re, " $1 ");

            normalizedWhere.erase(0, normalizedWhere.find_first_not_of(" \t\n\r\f\v"));
            normalizedWhere.erase(normalizedWhere.find_last_not_of(" \t\n\r\f\v") + 1);
        }

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

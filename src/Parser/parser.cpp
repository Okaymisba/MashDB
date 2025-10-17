#include "parser.h"
#include "../Creation/createDatabase.h"
#include "../Operations/ChangeDB/changeDB.h"
#include "../Operations/Insertion/insert.h"

#include <regex>
#include <iostream>
#include <algorithm>
#include <sstream>

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

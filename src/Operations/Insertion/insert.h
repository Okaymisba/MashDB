#pragma once

#include <string>
#include <vector>
#include <nlohmann/json.hpp>

using namespace std;
using json = nlohmann::json;

class InsertIntoTable {
public:
    static void insert(
        const string &databaseName,
        const string &tableName,
        const vector<string> &columns,
        const vector<json> &values
    );
};

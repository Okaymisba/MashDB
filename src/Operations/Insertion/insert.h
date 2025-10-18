#pragma once

#include <string>
#include <vector>

using namespace std;

class InsertIntoTable {
public:
    static void insert(
        const string &databaseName,
        const string &tableName,
        const vector<string> &columns,
        const vector<string> &values
    );
};

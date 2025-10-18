#pragma once
#include <string>
#include <vector>

using namespace std;

class CreateTable {
public:
    static void createTable(const string &tableName,
                            const vector<string> &columns,
                            const vector<string> &dataTypes,
                            const vector<bool> &isUnique,
                            const vector<bool> &notNull);
};

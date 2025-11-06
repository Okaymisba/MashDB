#pragma once
#include <string>

using namespace std;

class DeleteRow {
public:
    static void deleteRow(const string &tableName, const string &condition);
};

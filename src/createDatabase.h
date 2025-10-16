#pragma once
#include <string>
#include <vector>

class CreateDatabase
{
public:
    static void createDatabase(const std::string &databaseName);
    static void createTable(const std::string &tableName,
                            const std::vector<std::string> &columns,
                            const std::vector<std::string> &dataTypes,
                            const std::vector<bool> &isUnique,
                            const std::vector<bool> &notNull);
};

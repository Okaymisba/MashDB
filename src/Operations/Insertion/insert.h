#ifndef INSERTINTOTABLE_H
#define INSERTINTOTABLE_H

#include <string>
#include <vector>

class InsertIntoTable {
public:
    static void insert(
        const std::string &databaseName,
        const std::string &tableName,
        const std::vector<std::string> &columns,
        const std::vector<std::string> &values
    );
};

#endif

#ifndef MASHDB_UPDATEROW_H
#define MASHDB_UPDATEROW_H

#include <string>
#include <vector>
#include <unordered_map>
#include <nlohmann/json.hpp>
#include <functional>

using namespace std;
using json = nlohmann::json;

namespace UpdateOperation {
    /**
     * @brief Updates rows in a table that match the given conditions
     * @param tableName Name of the table to update
     * @param updates Map of column names to their new values
     * @param condition Optional condition string to filter which rows to update
     * @return int Number of rows updated, or -1 on error
     */
    int updateTable(
        const string &tableName,
        const unordered_map<string, json> &updates,
        const string &condition = ""
    );

    /**
     * @brief Validates if the update operation can be performed
     * @param tableName Name of the table
     * @param updates Map of column names to their new values
     * @return bool True if the update is valid, false otherwise
     */
    bool validateUpdate(
        const string &tableName,
        const unordered_map<string, json> &updates
    );
}

#endif

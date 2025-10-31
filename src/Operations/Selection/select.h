#pragma once

#include <string>
#include <vector>
#include <nlohmann/json.hpp>
#include <optional>
#include <functional>

using namespace std;
using json = nlohmann::json;

namespace Selection {
    /**
     * @brief Executes a SELECT query on the specified table
     *
     * @param databaseName Name of the database
     * @param tableName Name of the table to query
     * @param columns List of columns to select (empty for all columns)
     * @param whereCondition Optional condition function to filter rows
     * @param orderByColumn Optional column name to order results by
     * @param ascending Sort order (true = ascending, false = descending)
     * @param limit Optional maximum number of rows to return
     * @param offset Optional number of rows to skip
     * @return json Array of selected rows with specified columns
     */
    json selectFromTable(
        const string &databaseName,
        const string &tableName,
        const vector<string> &columns = {},
        const function<bool(const json &)> &whereCondition = nullptr,
        const string &orderByColumn = "",
        bool ascending = true,
        optional<size_t> limit = nullopt,
        size_t offset = 0
    );
} // namespace Selection

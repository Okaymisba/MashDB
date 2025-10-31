#include "select.h"
#include <fstream>
#include <filesystem>
#include <algorithm>
#include <stdexcept>

using namespace std;
using json = nlohmann::json;
namespace fs = filesystem;

namespace Selection {
    /**
     * @brief Helper function to load a column from disk
     */
    static json loadColumn(const string &filePath) {
        ifstream file(filePath);
        if (!file.is_open()) {
            throw runtime_error("Failed to open column file: " + filePath);
        }

        string content((istreambuf_iterator<char>(file)), istreambuf_iterator<char>());
        file.close();

        return content.empty() ? json::object() : json::parse(content);
    }

    /**
     * @brief Gets all column names for a table
     */
    static vector<string> getTableColumns(const string &tableInfoPath) {
        ifstream infoFile(tableInfoPath);
        if (!infoFile.is_open()) {
            throw runtime_error("Table-info.json not found");
        }

        json tableInfo;
        infoFile >> tableInfo;
        infoFile.close();

        vector<string> columns;
        for (auto &el: tableInfo.items()) {
            columns.push_back(el.key());
        }
        return columns;
    }

    /**
     * @brief Implements the SELECT operation with filtering, sorting, and pagination
     */
    json selectFromTable(
        const string &databaseName,
        const string &tableName,
        const vector<string> &columns,
        const function<bool(const json &)> &whereCondition,
        const string &orderByColumn,
        bool ascending,
        optional<size_t> limit,
        size_t offset
    ) {
        string basePath = fs::current_path().string() + "/Databases/" + databaseName + "/" + tableName;
        string infoFilePath = basePath + "/Table-info.json";

        // This is validating if the table exists
        if (!fs::exists(basePath) || !fs::exists(infoFilePath)) {
            throw runtime_error("Table doesn't exist");
        }

        // Get all columns if none specified
        auto allColumns = getTableColumns(infoFilePath);
        vector<string> selectedColumns = columns.empty() ? allColumns : columns;

        // This part is validating if the requested column exists or not
        for (const auto &col: selectedColumns) {
            if (find(allColumns.begin(), allColumns.end(), col) == allColumns.end()) {
                throw runtime_error("Column doesn't exist: " + col);
            }
        }

        // This is loading all the required columns in columnData
        map<string, json> columnData;
        for (const auto &col: selectedColumns) {
            string colPath = basePath + "/Columns/" + col + ".json";
            columnData[col] = loadColumn(colPath)[col];
        }

        // If ordering is requested then load the order column if not already loaded
        if (!orderByColumn.empty() && columnData.find(orderByColumn) == columnData.end()) {
            string orderColPath = basePath + "/Columns/" + orderByColumn + ".json";
            columnData[orderByColumn] = loadColumn(orderColPath)[orderByColumn];
        }

        // Determine the number of rows
        size_t rowCount = 0;
        if (!columnData.empty()) {
            rowCount = columnData.begin()->second.size();
        }

        // This creates a result set
        json result = json::array();
        vector<size_t> rowIndices(rowCount);

        // Initialize row indices
        for (size_t i = 0; i < rowCount; ++i) {
            rowIndices[i] = i;
        }

        // Apply ordering if requested
        if (!orderByColumn.empty()) {
            auto &orderColumn = columnData[orderByColumn];
            sort(rowIndices.begin(), rowIndices.end(),
                 [&](size_t a, size_t b) {
                     return ascending ? orderColumn[a] < orderColumn[b] : orderColumn[a] > orderColumn[b];
                 });
        }

        // Apply filtering and build result
        size_t skipped = 0;
        size_t returned = 0;

        for (size_t i = 0; i < rowCount; ++i) {
            size_t rowIdx = rowIndices[i];
            bool includeRow = true;

            // Apply WHERE condition if provided
            if (whereCondition) {
                json rowData;
                for (const auto &[col, data]: columnData) {
                    rowData[col] = data[rowIdx];
                }
                if (!whereCondition(rowData)) {
                    includeRow = false;
                }
            }

            // Skip if row doesn't match WHERE condition
            if (!includeRow) {
                continue;
            }

            // Handle OFFSET (skip first N rows)
            if (skipped < offset) {
                skipped++;
                continue;
            }

            // Handle LIMIT (return at most N rows)
            if (limit.has_value() && returned >= limit.value()) {
                break;
            }

            json row;

            // Add selected columns to result
            for (const auto &col: selectedColumns) {
                row[col] = columnData[col][rowIdx];
            }

            result.push_back(row);
            returned++;
        }

        return result;
    }
}

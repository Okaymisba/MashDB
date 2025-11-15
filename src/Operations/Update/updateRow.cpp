#include "updateRow.h"
#include "../../Parser/conditionParser.h"
#include "../CurrentDB/currentDB.h"

#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <nlohmann/json.hpp>
#include <iostream>

using namespace std;
namespace fs = filesystem;
using json = nlohmann::json;

static string currentDatabase = CurrentDB::getCurrentDB();

namespace UpdateOperation {
    /**
     * @brief Updates rows in a table that match the given condition
     * @param tableName Name of the table to update
     * @param updates Map of column names to their new values
     * @param conditionStr Optional condition string to filter which rows to update
     * @return int Number of rows updated, or -1 on error
     */
    int updateTable(
        const string &tableName,
        const unordered_map<string, json> &updates,
        const string &conditionStr
    ) {
        if (currentDatabase.empty()) {
            throw runtime_error("No database selected. Use 'USE DATABASE' first.");
        }

        fs::path basePath = fs::current_path() / "Databases" / currentDatabase / tableName;
        fs::path tableDir = basePath / "Columns";
        fs::path tableInfoFile = basePath / "Table-info.json";

        vector<pair<string, string> > staged;
        int updatedCount = 0;

        if (!fs::exists(tableDir)) {
            throw runtime_error("Table does not exist: " + tableName);
        }

        // Parse condition if provided
        Condition condition;
        if (!conditionStr.empty()) {
            try {
                condition = ConditionParser::parseCondition(conditionStr);
            } catch (const exception &e) {
                throw runtime_error("Invalid condition: " + string(e.what()));
            }
        }

        // Read table info
        json tableInfo;
        {
            ifstream tfile(tableInfoFile);
            if (!tfile) throw runtime_error("Failed to open table info file");
            tfile >> tableInfo;
        }

        // Validate updates against table schema
        for (const auto &[colName, _]: updates) {
            if (!tableInfo.contains(colName)) {
                throw runtime_error("Column not found in table: " + colName);
            }
        }

        // First, determine which rows match the condition by checking the condition column
        vector<bool> rowsToUpdate;
        size_t totalRows = 0;

        // Find the condition column data if there's a condition
        if (!conditionStr.empty()) {
            fs::path condColPath = tableDir / (condition.column + ".json");
            if (!fs::exists(condColPath)) {
                throw runtime_error("Condition column not found: " + condition.column);
            }

            json condColData;
            {
                ifstream cfile(condColPath);
                if (!cfile) throw runtime_error("Failed to open condition column file: " + condition.column);
                cfile >> condColData;
            }

            if (!condColData.contains(condition.column) || !condColData[condition.column].is_array()) {
                throw runtime_error("Invalid condition column data format");
            }

            const auto &condValues = condColData[condition.column];
            totalRows = condValues.size();
            rowsToUpdate.resize(totalRows, false);

            // Mark which rows should be updated based on the condition
            for (size_t i = 0; i < totalRows; ++i) {
                try {
                    rowsToUpdate[i] = ConditionParser::evaluateCondition(condValues[i], condition);
                    if (rowsToUpdate[i]) {
                        updatedCount++;
                    }
                } catch (const exception &e) {
                    cerr << "Error evaluating condition for row " << i
                            << " in column " << condition.column << ": " << e.what() << endl;
                    rowsToUpdate[i] = false;
                }
            }
        } else {
            // If no condition, update all rows
            // Get the row count from the first column
            fs::path firstColPath = tableDir / (tableInfo.begin().key() + ".json");
            if (fs::exists(firstColPath)) {
                json firstColData;
                ifstream ffile(firstColPath);
                if (ffile) {
                    ffile >> firstColData;
                    if (firstColData.contains(tableInfo.begin().key()) &&
                        firstColData[tableInfo.begin().key()].is_array()) {
                        totalRows = firstColData[tableInfo.begin().key()].size();
                        rowsToUpdate.resize(totalRows, true);
                        updatedCount = totalRows;
                    }
                }
            }
        }

        // Now process each column that needs to be updated
        for (const auto &colName: updates) {
            fs::path colPath = tableDir / (colName.first + ".json");
            if (!fs::exists(colPath)) {
                throw runtime_error("Column not found: " + colName.first);
            }

            // Read column data
            json colData;
            {
                ifstream cfile(colPath);
                if (!cfile) throw runtime_error("Failed to open column file: " + colName.first);
                cfile >> colData;
            }

            if (!colData.contains(colName.first) || !colData[colName.first].is_array()) {
                throw runtime_error("Invalid column data format for: " + colName.first);
            }

            auto &values = colData[colName.first];
            bool isUpdated = false;

            // Update the values for rows that match the condition
            for (size_t i = 0; i < values.size() && i < rowsToUpdate.size(); ++i) {
                if (rowsToUpdate[i]) {
                    // Only update if the value is different
                    if (values[i] != colName.second) {
                        values[i] = colName.second;
                        isUpdated = true;
                    }
                }
            }

            // If column was updated, stage it for writing
            if (isUpdated) {
                fs::path tempPath = colPath.string() + ".tmp";
                {
                    ofstream outFile(tempPath, ios::trunc);
                    if (!outFile) throw runtime_error("Failed to create temporary file for column: " + colName.first);
                    outFile << colData.dump(4);
                }
                staged.emplace_back(tempPath.string(), colPath.string());
            }
        }

        // Apply all updates if no errors occurred
        if (!staged.empty()) {
            try {
                for (const auto &[tempPath, finalPath]: staged) {
                    fs::rename(tempPath, finalPath);
                }
                return updatedCount;
            } catch (const exception &e) {
                // Clean up any temporary files on error
                for (const auto &[tempPath, _]: staged) {
                    if (fs::exists(tempPath)) {
                        fs::remove(tempPath);
                    }
                }
                throw runtime_error("Failed to apply updates: " + string(e.what()));
            }
        }

        return updatedCount;
    }
} // namespace UpdateOperation

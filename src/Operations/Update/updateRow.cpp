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
     * @brief Updates rows in a table based on a given condition.
     *
     * If a condition string is provided, this function will update all rows in the table
     * that satisfy the condition. If no condition string is provided, all rows in the table
     * will be updated.
     *
     * @param tableName The name of the table to be updated.
     * @param updates A map of column names to the values to be written to those columns.
     * @param conditionStr An optional condition string to filter which rows are updated.
     * @return The number of rows updated.
     * @throws std::runtime_error If the table does not exist, if the column specified in the condition
     * does not exist, if the format of the target column or table information is invalid, if
     * opening or writing files associated with the table fails, or if errors occur during condition
     * evaluation or deletion processing.
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

        Condition condition;
        if (!conditionStr.empty()) {
            try {
                condition = ConditionParser::parseCondition(conditionStr);
            } catch (const exception &e) {
                throw runtime_error("Invalid condition: " + string(e.what()));
            }
        }

        json tableInfo;
        {
            ifstream tfile(tableInfoFile);
            if (!tfile) throw runtime_error("Failed to open table info file");
            tfile >> tableInfo;
        }

        for (const auto &[colName, _]: updates) {
            if (!tableInfo.contains(colName)) {
                throw runtime_error("Column not found in table: " + colName);
            }
        }

        vector<bool> rowsToUpdate;
        size_t totalRows = 0;

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

        for (const auto &colName: updates) {
            fs::path colPath = tableDir / (colName.first + ".json");
            if (!fs::exists(colPath)) {
                throw runtime_error("Column not found: " + colName.first);
            }

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

            for (size_t i = 0; i < values.size() && i < rowsToUpdate.size(); ++i) {
                if (rowsToUpdate[i]) {
                    if (values[i] != colName.second) {
                        values[i] = colName.second;
                        isUpdated = true;
                    }
                }
            }

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

        if (!staged.empty()) {
            try {
                for (const auto &[tempPath, finalPath]: staged) {
                    fs::rename(tempPath, finalPath);
                }
                return updatedCount;
            } catch (const exception &e) {
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
}

#include "deleteRow.h"
#include "../../Parser/conditionParser.h"

#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <nlohmann/json.hpp>
#include <iostream>

#include "../CurrentDB/currentDB.h"

using namespace std;
namespace fs = filesystem;
using json = nlohmann::json;

static string currentDatabase = CurrentDB::getCurrentDB();

/**
 * @brief Deletes rows from a specified table in the database based on a given condition.
 *
 * This function performs deletion of rows from the specified table by evaluating a
 * condition against a target column. If the condition evaluates to true for specific rows,
 * those rows are deleted across all columns in the table. The deletion process ensures
 * data consistency by processing updates via temporary files, and managing cleanup in
 * the event of errors.
 *
 * @param tableName The name of the table from which rows are to be deleted.
 * @param conditionStr The condition string defining which rows to delete, parsed into a
 *                     `Condition` object.
 *
 * @throws std::runtime_error If any of the following errors occur:
 * - The table does not exist.
 * - The column specified in the condition does not exist.
 * - The format of the target column or table information is invalid.
 * - Failure to open or write files associated with the table.
 * - Errors during condition evaluation or deletion processing.
 *
 * @details
 * - The function begins by parsing the condition string into a `Condition` object using
 *   a condition parser.
 * - It verifies the existence of the table and retrieves the table's column information
 *   stored in a `Table-info.json` file.
 * - The target column is then checked for rows that match the condition. These row indices
 *   are collected and sorted in descending order for deletion, avoiding index shift issues
 *   during row removal.
 * - Once rows to delete are identified, all table columns are processed to remove the same
 *   rows, ensuring integrity across the table.
 * - Updates are written to temporary files first, which are renamed to the final column
 *   files only after all deletions succeed.
 * - In case of any error, temporary files created during the process are cleaned up to
 *   prevent partial updates.
 */
void DeleteRow::deleteRow(const string &tableName, const string &conditionStr) {
    Condition condition;
    try {
        condition = ConditionParser::parseCondition(conditionStr);
    } catch (const std::exception &e) {
        throw std::runtime_error("Invalid condition: " + std::string(e.what()));
    }
    fs::path homeDir = getenv("HOME");
    if (homeDir.empty()) homeDir = getenv("USERPROFILE");
    fs::path basePath = homeDir / ".mashdb" / "databases" / currentDatabase / tableName;
    fs::path tableDir = basePath / "Columns";
    fs::path tableInfoFile = basePath / "Table-info.json";

    vector<pair<string, string> > staged;

    if (!fs::exists(tableDir))
        throw runtime_error("Table does not exist.");

    json columnInfoJson;
    ifstream tfile(tableInfoFile);
    if (!tfile)
        throw runtime_error("Failed to open table info file.");
    tfile >> columnInfoJson;
    tfile.close();

    if (!columnInfoJson.contains(condition.column)) {
        throw runtime_error("Column not found in table: " + condition.column);
    }

    vector<size_t> rowsToDelete;
    {
        const std::string &targetCol = condition.column;
        fs::path targetPath = tableDir / (targetCol + ".json");

        if (!fs::exists(targetPath)) {
            throw runtime_error("Target column file does not exist: " + targetCol);
        }

        json targetData;
        {
            ifstream tfile(targetPath);
            if (!tfile) throw runtime_error("Failed to open target column file: " + targetCol);
            tfile >> targetData;
        }

        if (!targetData.contains(targetCol) || !targetData[targetCol].is_array()) {
            throw runtime_error("Invalid target column data format");
        }

        const auto &values = targetData[targetCol];

        for (size_t i = 0; i < values.size(); ++i) {
            try {
                if (ConditionParser::evaluateCondition(values[i], condition)) {
                    rowsToDelete.push_back(i);
                }
            } catch (const exception &e) {
                cerr << "Error evaluating condition for row " << i
                        << " in column " << targetCol << ": " << e.what() << endl;
            }
        }

        if (rowsToDelete.empty()) {
            cout << "No rows match the condition. Nothing to delete." << endl;
            return;
        }

        sort(rowsToDelete.rbegin(), rowsToDelete.rend());
        rowsToDelete.erase(unique(rowsToDelete.begin(), rowsToDelete.end()), rowsToDelete.end());

        cout << "Found " << rowsToDelete.size() << " rows to delete." << endl;
    }

    try {
        for (auto it = columnInfoJson.begin(); it != columnInfoJson.end(); ++it) {
            const std::string &colName = it.key();
            fs::path finalPath = tableDir / (colName + ".json");
            if (!fs::exists(finalPath))
                throw runtime_error("Column file does not exist: " + colName);

            json columnData;
            {
                ifstream cfile(finalPath);
                if (!cfile)
                    throw runtime_error("Failed to open column file: " + colName);
                cfile >> columnData;
                cfile.close();
            }

            if (!columnData.contains(colName))
                throw runtime_error("Invalid column data: missing key '" + colName + "'");
            auto &arr = columnData[colName];
            if (!arr.is_array())
                throw runtime_error("Invalid column data: expected array for column '" + colName + "'");

            size_t initialSize = arr.size();
            if (!rowsToDelete.empty()) {
                for (size_t row: rowsToDelete) {
                    if (row < arr.size()) {
                        arr.erase(arr.begin() + row);
                    } else {
                        cerr << "Warning: Row index " << row << " out of bounds for column " << colName << endl;
                    }
                }
            }

            if (arr.size() < initialSize) {
                fs::path tempPath = finalPath.string() + ".tmp";
                {
                    ofstream outFile(tempPath, ios::trunc);
                    if (!outFile)
                        throw runtime_error("Failed to create temporary file for column: " + colName);
                    outFile << columnData.dump(4);
                    outFile.close();
                }
                staged.emplace_back(tempPath.string(), finalPath.string());
            }
        }

        for (const auto &[tempPath, finalPath]: staged) {
            fs::rename(tempPath, finalPath);
        }
    } catch (const exception &e) {
        for (const auto &[tempPath, _]: staged) {
            if (fs::exists(tempPath)) {
                fs::remove(tempPath);
            }
        }
        throw;
    }
}

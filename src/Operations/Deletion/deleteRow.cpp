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
 * @brief Removes rows from a table where column values match the given condition.
 *
 * This function handles the heavy lifting of deleting rows from our database.
 * It's careful about data integrity - first writing changes to temp files and
 * only committing them when we're sure everything worked. This way, if something
 * crashes halfway through, we don't end up with half-deleted data.
 *
 * @param tableName Name of the table we're working with
 * @param condition The value we're looking to match and remove
 *
 * @throws std::runtime_error If anything goes wrong - like if the table doesn't exist,
 *         we can't read/write the files, or the data looks corrupted.
 */
void DeleteRow::deleteRow(const string &tableName, const string &conditionStr) {
    // Parse the condition string
    Condition condition;
    try {
        condition = ConditionParser::parseCondition(conditionStr);
    } catch (const std::exception &e) {
        throw std::runtime_error("Invalid condition: " + std::string(e.what()));
    }
    fs::path basePath = fs::current_path() / "Databases" / currentDatabase / tableName;
    fs::path tableDir = basePath / "Columns";
    fs::path tableInfoFile = basePath / "Table-info.json";
    // Keep track of our temp files so we can clean them up later
    vector<pair<string, string> > staged;

    if (!fs::exists(tableDir))
        throw runtime_error("Table does not exist.");

    json columnInfoJson;
    ifstream tfile(tableInfoFile);
    if (!tfile)
        throw runtime_error("Failed to open table info file.");
    tfile >> columnInfoJson;
    tfile.close();

    try {
        for (const auto &column: columnInfoJson) {
            if (!column.contains("name")) {
                throw runtime_error("Invalid table info: column missing 'name' field");
            }

            const std::string colName = column["name"].get<std::string>();

            fs::path finalPath = tableDir / (colName + ".json");
            if (!fs::exists(finalPath))
                throw runtime_error("Column does not exist: " + colName);

            // Grab the current column data from disk
            json columnData;
            {
                ifstream cfile(finalPath);
                if (!cfile)
                    throw runtime_error("Failed to open column file: " + colName);
                cfile >> columnData;
                cfile.close();
            }

            // Make sure the data looks right before we start messing with it
            if (!columnData.contains(colName))
                throw runtime_error("Invalid column data: missing key '" + colName + "'");
            auto &arr = columnData[colName];
            if (!arr.is_array())
                throw runtime_error("Invalid column data: expected array for column '" + colName + "'");

            // For the column we're filtering on, collect matching row indices
            static vector<size_t> rowsToDelete;
            if (colName == condition.column) {
                rowsToDelete.clear();

                // First, check if the column exists in the data
                if (!columnData.contains(colName)) {
                    throw runtime_error("Column not found: " + colName);
                }

                // Get the array of values for this column
                const auto &values = columnData[colName];
                if (!values.is_array()) {
                    throw runtime_error("Invalid data format for column: " + colName);
                }

                // Process each value in the column
                for (size_t i = 0; i < values.size(); ++i) {
                    try {
                        // Pass the actual JSON value to evaluateCondition
                        if (ConditionParser::evaluateCondition(values[i], condition)) {
                            rowsToDelete.push_back(i);
                        }
                    } catch (const std::exception &e) {
                        // Log the error but continue with other rows
                        cerr << "Error evaluating condition for row " << i
                                << " in column " << colName << ": " << e.what() << endl;
                        continue;
                    }
                }

                // If no rows match the condition, we're done
                if (rowsToDelete.empty()) {
                    return;
                }
            }

            // Remove the marked rows (in reverse order to avoid index shifting issues)
            size_t initialSize = arr.size();
            if (!rowsToDelete.empty()) {
                // Sort in descending order for safe removal
                std::sort(rowsToDelete.rbegin(), rowsToDelete.rend());
                // Remove duplicates just in case
                rowsToDelete.erase(std::unique(rowsToDelete.begin(), rowsToDelete.end()), rowsToDelete.end());

                // Remove rows
                for (size_t row: rowsToDelete) {
                    if (row < arr.size()) {
                        arr.erase(arr.begin() + row);
                    }
                }
            }

            // Only create temp files for columns that actually changed
            if (arr.size() < initialSize) {
                // Write changes to a temp file first
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

        // If we made it here, everything worked Now make the changes permanent
        for (const auto &[tempPath, finalPath]: staged) {
            fs::rename(tempPath, finalPath);
        }
    } catch (const exception &e) {
        // If some exception occurs, clean up any temp files we created
        for (const auto &[tempPath, _]: staged) {
            if (fs::exists(tempPath)) {
                fs::remove(tempPath);
            }
        }
        throw; // Re-throw the exception after cleanup
    }
}

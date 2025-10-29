#include "insert.h"
#include <fstream>
#include <iostream>
#include <filesystem>
#include <nlohmann/json.hpp>

using namespace std;
using json = nlohmann::json;
namespace fs = filesystem;

/**
 * Inserts a new record into the specified table using a temp-file staging strategy
 * to prevent partial writes. The logic preserves the existing per-column loop but
 * ensures atomicity at the table level:
 *
 * Algorithm overview (per INSERT):
 * 1) Validate request shape:
 *    - Table and metadata existence (e.g., `Table-info.json`).
 *    - Column existence and columns/values size match.
 * 2) For each table column (in a loop):
 *    - Load the column's current JSON data.
 *    - Validate constraints using in-memory data:
 *        - notNull: if the column is not provided and marked not-null, fail.
 *        - isUnique: if value is provided and already present, fail.
 *        - Type: a placeholder exists to enforce/convert by `type` if desired.
 *    - Apply the value in memory (append provided value or nullptr for nullable-missing).
 *    - Write the updated JSON for this column to a sibling temp file `<col>.json.tmp`.
 * 3) Commit phase (only if every temp write succeeded):
 *    - Atomically replace each original file with its temp file via `fs::rename(tmp, final)`.
 * 4) Rollback on failure:
 *    - If any validation or write fails, delete all created temp files and rethrow.
 *    - Original column files remain untouched, so no partial insert occurs.
 *
 * Guarantees and caveats:
 * - Atomicity: No original files are changed unless all columns succeed and renames complete.
 * - Crash safety: This protects against logical/validation errors during a single INSERT.
 *   To be resilient against process crash or power loss mid-commit, consider journaling
 *   or a two-phase rename with a table-level marker.
 * - Concurrency: For strict isolation across concurrent INSERTs, add a table-level lock.
 * - Type enforcement: Currently a TODO; values are treated as strings for uniqueness.
 *
 * @param databaseName Name of the database containing the target table.
 * @param tableName Name of the table where data is to be inserted.
 * @param columns List of column names corresponding to the values being inserted.
 * @param values List of values to be inserted into the specified columns.
 * @throws std::runtime_error If the table does not exist in the database.
 * @throws std::runtime_error If the table information file is missing.
 * @throws std::runtime_error If the number of columns and values do not match.
 * @throws std::runtime_error If too many columns are provided.
 * @throws std::runtime_error If a specified column does not exist in the table.
 * @throws std::runtime_error If a non-nullable column is assigned a null value.
 * @throws std::runtime_error If a duplicate value is provided for a unique column.
 * @throws std::runtime_error If a required column file is missing.
 */
void InsertIntoTable::insert(
    const string &databaseName,
    const string &tableName,
    const vector<string> &columns,
    const vector<string> &values
) {
    string basePath = fs::current_path().string() + "/Databases/" + databaseName + "/" + tableName;
    string infoFilePath = basePath + "/Table-info.json";

    if (!fs::exists(basePath)) {
        throw runtime_error("Table doesn't exist");
    }

    ifstream infoFile(infoFilePath);
    if (!infoFile.is_open()) {
        throw runtime_error("Table-info.json not found");
    }

    json tableInfo;
    infoFile >> tableInfo;
    infoFile.close();

    vector<string> columnsOfTable;
    for (auto &el: tableInfo.items()) {
        columnsOfTable.push_back(el.key());
    }

    if (columns.size() != values.size())
        throw runtime_error("Must initialize value for every column");

    if (columns.size() > columnsOfTable.size())
        throw runtime_error("Too many columns");

    for (const auto &col: columns) {
        if (find(columnsOfTable.begin(), columnsOfTable.end(), col) == columnsOfTable.end())
            throw runtime_error("Column doesn't exist: " + col);
    }

    vector<pair<string, string> > staged;

    try {
        for (const auto &column: columnsOfTable) {
            string finalPath = basePath + "/Columns/" + column + ".json";
            if (!fs::exists(finalPath))
                throw runtime_error("Missing column file: " + column);

            ifstream colFile(finalPath);
            string content((istreambuf_iterator<char>(colFile)), istreambuf_iterator<char>());
            colFile.close();

            json colJson;
            if (!content.empty()) {
                colJson = json::parse(content);
            } else {
                colJson = json::object();
                colJson[column] = json::array();
            }

            json colInfo = tableInfo[column];
            string type = colInfo["type"];
            bool isUnique = colInfo["isUnique"];
            bool notNull = colInfo["notNull"];

            auto &dataArray = colJson[column];
            int index = -1;
            for (size_t i = 0; i < columns.size(); ++i) {
                if (columns[i] == column) {
                    index = i;
                    break;
                }
            }

            if (index == -1) {
                if (notNull)
                    throw runtime_error("Value cannot be null for column: " + column);
                else
                    dataArray.push_back(nullptr);
            } else {
                string val = values[index];
                // TODO: Optionally enforce 'type' here before uniqueness
                if (isUnique && find(dataArray.begin(), dataArray.end(), val) != dataArray.end()) {
                    throw runtime_error("Duplicate value for unique column: " + column);
                }
                dataArray.push_back(val);
            }

            string tempPath = finalPath + ".tmp";
            {
                ofstream outFile(tempPath, ios::trunc);
                outFile << colJson.dump(4);
                outFile.close();
            }

            staged.emplace_back(tempPath, finalPath);
        }

        for (const auto &p: staged) {
            const string &tempPath = p.first;
            const string &finalPath = p.second;
            fs::rename(tempPath, finalPath);
        }
    } catch (const exception &e) {
        for (const auto &p: staged) {
            const string &tempPath = p.first;
            if (fs::exists(tempPath)) {
                fs::remove(tempPath);
            }
        }
        throw;
    }
}

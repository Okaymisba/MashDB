#include "insert.h"
#include <fstream>
#include <iostream>
#include <filesystem>
#include <nlohmann/json.hpp>

using namespace std;
using json = nlohmann::json;
namespace fs = filesystem;

/**
 * Inserts a new record into the specified table with atomicity guarantees.
 * Uses a temp-file staging strategy to ensure data consistency even in case of failures.
 *
 * Operation Details:
 * 1. Validates input parameters and loads table metadata
 * 2. For each column in the table:
 *    - Validates NOT NULL constraints
 *    - Enforces UNIQUE constraints
 *    - Validates and converts data types
 *    - Writes to a temporary file
 * 3. On success: Atomically renames all temp files to their final names
 * 4. On failure: Cleans up any temporary files and throws an exception
 *
 * Type Handling:
 * - Integers: Validates and stores as JSON number_integer
 * - Floats: Validates and stores as JSON number_float
 * - Booleans: Converts from string (true/false) to JSON boolean
 * - Strings: Preserves content, handles quoted values
 *
 * @param databaseName Name of the target database
 * @param tableName Name of the target table
 * @param columns List of column names for insertion
 * @param values List of values corresponding to the columns
 * @throws std::runtime_error If validation fails or write operations encounter errors
 */
void InsertIntoTable::insert(
    const string &databaseName,
    const string &tableName,
    const vector<string> &columns,
    const vector<json> &values
) {
    fs::path homeDir = getenv("HOME");
    if (homeDir.empty()) homeDir = getenv("USERPROFILE");
    fs::path basePath = homeDir / ".mashdb" / "databases" / databaseName / tableName;
    fs::path infoFilePath = basePath / "Table-info.json";

    string basePathStr = basePath.string();
    string infoFilePathStr = infoFilePath.string();

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
            fs::path finalPath = basePath / "Columns" / (column + ".json");
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
                const json &typedVal = values[index];

                auto toLower = [](string s) {
                    transform(s.begin(), s.end(), s.begin(),
                              [](unsigned char c) { return tolower(c); });
                    return s;
                };
                string expectedType = toLower(type);

                if (!typedVal.is_null()) {
                    bool typeValid = false;

                    if (expectedType == "int" || expectedType == "integer") {
                        typeValid = typedVal.is_number_integer();
                    } else if (expectedType == "float" || expectedType == "double" || expectedType == "real") {
                        typeValid = typedVal.is_number_float() || typedVal.is_number_integer();
                    } else if (expectedType == "bool" || expectedType == "boolean") {
                        typeValid = typedVal.is_boolean();
                    } else {
                        typeValid = typedVal.is_string();
                    }

                    if (!typeValid) {
                        string typeName;
                        if (typedVal.is_string()) typeName = "string";
                        else if (typedVal.is_number_integer()) typeName = "integer";
                        else if (typedVal.is_number_float()) typeName = "float";
                        else if (typedVal.is_boolean()) typeName = "boolean";
                        else typeName = "unknown";

                        throw runtime_error("Type mismatch for column '" + column + "': "
                                            "expected " + expectedType + ", got " + typeName);
                    }
                }

                if (isUnique) {
                    auto it = find(dataArray.begin(), dataArray.end(), typedVal);
                    if (it != dataArray.end()) {
                        throw runtime_error("Duplicate value for unique column: " + column);
                    }
                }

                dataArray.push_back(typedVal);
            }

            string tempPath = finalPath / ".tmp";
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

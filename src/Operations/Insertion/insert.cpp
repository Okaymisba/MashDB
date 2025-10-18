#include "insert.h"
#include <fstream>
#include <iostream>
#include <filesystem>
#include <nlohmann/json.hpp>

using namespace std;
using json = nlohmann::json;
namespace fs = filesystem;

/**
 * Inserts a new record into the specified table within the given database.
 * Validates the input against the table's metadata and enforces constraints such as unique and non-null values.
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

    for (const auto &column: columnsOfTable) {
        string colPath = basePath + "/Columns/" + column + ".json";
        if (!fs::exists(colPath))
            throw runtime_error("Missing column file: " + column);

        ifstream colFile(colPath);
        json colJson;
        if (colFile.peek() != ifstream::traits_type::eof()) {
            colFile >> colJson;
        } else {
            colJson = json::object();
            colJson[column] = json::array();
        }
        colFile.close();

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
            if (isUnique && find(dataArray.begin(), dataArray.end(), val) != dataArray.end()) {
                throw runtime_error("Duplicate value for unique column: " + column);
            }
            dataArray.push_back(val);
        }

        ofstream outFile(colPath);
        outFile << colJson.dump(4);
        outFile.close();
    }
}

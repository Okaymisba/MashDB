#include "insert.h"
#include <fstream>
#include <iostream>
#include <filesystem>
#include <nlohmann/json.hpp>

using json = nlohmann::json;
namespace fs = std::filesystem;

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
    const std::string &databaseName,
    const std::string &tableName,
    const std::vector<std::string> &columns,
    const std::vector<std::string> &values
) {
    std::string basePath = fs::current_path().string() + "/Databases/" + databaseName + "/" + tableName;
    std::string infoFilePath = basePath + "/Table-info.json";

    if (!fs::exists(basePath)) {
        throw std::runtime_error("Table doesn't exist");
    }

    std::ifstream infoFile(infoFilePath);
    if (!infoFile.is_open()) {
        throw std::runtime_error("Table-info.json not found");
    }

    json tableInfo;
    infoFile >> tableInfo;
    infoFile.close();

    std::vector<std::string> columnsOfTable;
    for (auto &el: tableInfo.items()) {
        columnsOfTable.push_back(el.key());
    }

    if (columns.size() != values.size())
        throw std::runtime_error("Must initialize value for every column");

    if (columns.size() > columnsOfTable.size())
        throw std::runtime_error("Too many columns");

    for (const auto &col: columns) {
        if (std::find(columnsOfTable.begin(), columnsOfTable.end(), col) == columnsOfTable.end())
            throw std::runtime_error("Column doesn't exist: " + col);
    }

    // Insert logic
    for (const auto &column: columnsOfTable) {
        std::string colPath = basePath + "/Columns/" + column + ".json";
        if (!fs::exists(colPath))
            throw std::runtime_error("Missing column file: " + column);

        std::ifstream colFile(colPath);
        json colJson;
        if (colFile.peek() != std::ifstream::traits_type::eof()) {
            colFile >> colJson;
        } else {
            colJson = json::object();
            colJson[column] = json::array();
        }
        colFile.close();

        json colInfo = tableInfo[column];
        std::string type = colInfo["type"];
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
                throw std::runtime_error("Value cannot be null for column: " + column);
            else
                dataArray.push_back(nullptr);
        } else {
            std::string val = values[index];
            if (isUnique && std::find(dataArray.begin(), dataArray.end(), val) != dataArray.end()) {
                throw std::runtime_error("Duplicate value for unique column: " + column);
            }
            dataArray.push_back(val);
        }

        std::ofstream outFile(colPath);
        outFile << colJson.dump(4);
        outFile.close();
    }
}

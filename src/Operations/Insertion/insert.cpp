#include "insert.h"
#include <fstream>
#include <iostream>
#include <filesystem>
#include <nlohmann/json.hpp>

using json = nlohmann::json;
namespace fs = std::filesystem;

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

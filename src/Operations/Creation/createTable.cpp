#include "createTable.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <nlohmann/json.hpp>

#include "../CurrentDB/currentDB.h"

using namespace std;
namespace fs = filesystem;
using json = nlohmann::json;

static string currentDatabase = CurrentDB::getCurrentDB();


/**
* @brief Creates a new table in the database.
 *
 * This function initializes a table with the specified configuration and
 * adds it to the existing database structure.
 *
 */
void CreateTable::createTable(const std::string &tableName,
                              const std::vector<std::string> &columns,
                              const std::vector<std::string> &dataTypes,
                              const std::vector<bool> &isUnique,
                              const std::vector<bool> &notNull) {
    if (columns.size() != dataTypes.size())
        throw runtime_error("Must initialize Data Type for every Column.");

    fs::path homeDir = getenv("HOME");
    if (homeDir.empty()) homeDir = getenv("USERPROFILE");
    fs::path basePath = homeDir / ".mashdb" / "databases" / currentDatabase / tableName;
    fs::path tableDir = basePath / "Columns";
    fs::path tableInfoFile = basePath / "Table-info.json";

    if (!fs::exists(tableDir))
        fs::create_directories(tableDir);

    json columnInfoJson;

    for (size_t i = 0; i < columns.size(); ++i) {
        const string &column = columns[i];
        fs::path columnFile = tableDir / (column + ".json");

        if (!fs::exists(columnFile)) {
            ofstream cfile(columnFile);
            if (!cfile)
                throw runtime_error("Failed to create column file: " + column);
            json emptyCol = {{column, json::array()}};
            cfile << emptyCol.dump(4);
            cfile.close();
        }

        json columnData = {
            {"type", dataTypes[i]},
            {"isUnique", isUnique[i]},
            {"notNull", notNull[i]}
        };

        columnInfoJson[column] = columnData;
    }

    ofstream tfile(tableInfoFile);
    if (!tfile)
        throw runtime_error("Failed to create table info file.");
    tfile << columnInfoJson.dump(4);
    tfile.close();
}

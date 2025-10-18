#include "createDatabase.h"
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <nlohmann/json.hpp>

using namespace std;
namespace fs = filesystem;
using json = nlohmann::json;

static string currentDatabase;


/**
* @brief Creates a new table in the database.
 *
 * This function initializes a table with the specified configuration and
 * adds it to the existing database structure.
 *
 */
void CreateDatabase::createTable(const std::string &tableName,
                                 const std::vector<std::string> &columns,
                                 const std::vector<std::string> &dataTypes,
                                 const std::vector<bool> &isUnique,
                                 const std::vector<bool> &notNull) {
    if (columns.size() != dataTypes.size())
        throw runtime_error("Must initialize Data Type for every Column.");

    fs::path tableDir = fs::current_path() / "Databases" / currentDatabase / tableName / "Columns";
    fs::path tableInfoFile = fs::current_path() / "Databases" / currentDatabase / tableName / "Table-info.json";

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

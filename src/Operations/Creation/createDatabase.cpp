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
 * Creates a new database with the specified name.
 *
 * This function creates a new directory with the specified name under the ".mashdb/databases"
 * directory. If the directory already exists, a std::runtime_error is thrown.
 *
 * @param databaseName The name of the database to be created.
 * @throws std::runtime_error If the database already exists, or if the current database file
 * cannot be created.
 */
void CreateDatabase::createDatabase(const string &databaseName) {
    fs::path homeDir = getenv("HOME");
    if (homeDir.empty()) homeDir = getenv("USERPROFILE");
    fs::path basePath = homeDir / ".mashdb" / "databases" / databaseName;
    fs::path currentDbFile = homeDir / ".mashdb" / "crrtdb.txt";

    if (!fs::exists(basePath)) {
        fs::create_directories(basePath);
        currentDatabase = databaseName;

        if (!fs::exists(currentDbFile.parent_path()))
            fs::create_directories(currentDbFile.parent_path());

        ofstream file(currentDbFile);
        if (!file)
            throw runtime_error("Failed to create current database file.");
        file << currentDatabase;
        file.close();
    } else {
        throw runtime_error("Database already exists.");
    }
}

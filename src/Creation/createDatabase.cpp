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
 * @brief Creates a new database directory and updates the current database file.
 *
 * This function creates a new database directory in the file system at a
 * predefined path. If the database with the specified name already exists,
 * an exception is thrown. If not, it creates the necessary directory structure
 * and updates a separate file storing the name of the current database in use.
 *
 * @param databaseName The name of the database to be created.
 *
 * @throws std::runtime_error If the database already exists or if there is
 * a failure in creating directories or files.
 */
void CreateDatabase::createDatabase(const string &databaseName) {
    fs::path basePath = fs::current_path() / "Databases" / databaseName;
    fs::path currentDbFile = fs::current_path() / "crrtdb" / "crrtdb.txt";

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

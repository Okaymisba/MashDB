#include "changeDB.h"
#include <fstream>
#include <filesystem>

namespace fs = std::filesystem;

/**
 * Changes the current active database by updating the database reference file.
 * If the specified database does not exist, an exception is thrown.
 *
 * @param databaseName The name of the database to be set as the current active database.
 * @throws std::runtime_error If the specified database does not exist or if the reference file cannot be written.
 */
void ChangeDB::change(const std::string &databaseName) {
    std::string dbPath = fs::current_path().string() + "/Databases/" + databaseName;
    std::string currentDbFile = fs::current_path().string() + "/crrtdb/crrtdb.txt";

    if (!fs::exists(dbPath)) {
        throw std::runtime_error("No database with the name '" + databaseName + "' found");
    }

    fs::create_directories(fs::path(currentDbFile).parent_path());

    std::ofstream file(currentDbFile, std::ios::trunc);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file for writing: " + currentDbFile);
    }

    file << databaseName;
    file.close();
}

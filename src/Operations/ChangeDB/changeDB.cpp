#include "changeDB.h"
#include <fstream>
#include <filesystem>

namespace fs = std::filesystem;

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

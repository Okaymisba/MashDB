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
    try {
        fs::path homeDir = getenv("HOME");
        if (homeDir.empty()) homeDir = getenv("USERPROFILE");

        fs::path mashdbDir = homeDir / ".mashdb";
        fs::path databasesDir = mashdbDir / "databases";
        fs::path basePath = databasesDir / databaseName;
        fs::path currentDbFile = mashdbDir / "crrtdb.txt";

        if (fs::exists(basePath)) {
            throw runtime_error("Database '" + databaseName + "' already exists.");
        }

        if (!fs::exists(mashdbDir)) {
            if (!fs::create_directories(mashdbDir)) {
                throw runtime_error("Failed to create directory: " + mashdbDir.string());
            }
        }

        if (!fs::exists(databasesDir)) {
            if (!fs::create_directories(databasesDir)) {
                throw runtime_error("Failed to create directory: " + databasesDir.string());
            }
        }

        if (!fs::create_directories(basePath)) {
            throw runtime_error("Failed to create database directory: " + basePath.string());
        }

        currentDatabase = databaseName;

        ofstream file(currentDbFile, ios::trunc);
        if (!file.is_open()) {
            throw runtime_error("Failed to create/update current database file: " + currentDbFile.string());
        }
        file << currentDatabase;
        file.close();
    } catch (const fs::filesystem_error &e) {
        throw runtime_error("Filesystem error: " + string(e.what()));
    } catch (const exception &e) {
        throw runtime_error("Error creating database: " + string(e.what()));
    }
}

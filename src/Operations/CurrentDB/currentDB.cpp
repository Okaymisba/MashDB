#include "currentDB.h"
#include <fstream>
#include <iostream>
#include <filesystem>
#include <algorithm>

using namespace std;
namespace fs = filesystem;

/**
 * Retrieves the name or content of the current database from a predefined file path.
 *
 * This method reads the database's name or identifier from a text file located in the
 * "crrtdb/crrtdb.txt" file relative to the current working directory.
 *
 * @return A string containing the name or content of the current database. All newline
 *         and carriage return characters are removed from the content.
 * @throws std::runtime_error If the file cannot be opened, or if the file's content is empty.
 */
string CurrentDB::getCurrentDB() {
    try {
        fs::path homeDir = getenv("HOME");
        if (homeDir.empty()) homeDir = getenv("USERPROFILE");

        fs::path mashdbDir = homeDir / ".mashdb";
        fs::path path = mashdbDir / "crrtdb.txt";

        if (!fs::exists(path) || fs::is_empty(path)) {
            if (!fs::exists(mashdbDir)) {
                if (!fs::create_directories(mashdbDir)) {
                    throw runtime_error("Failed to create directory: " + mashdbDir.string());
                }
            }

            ofstream file(path);
            if (!file) {
                throw runtime_error("Failed to create current database file: " + path.string());
            }
            file.close();

            return "";
        }

        ifstream file(path);
        if (!file.is_open()) {
            throw runtime_error("Cannot open current database file: " + path.string());
        }

        string content((istreambuf_iterator<char>(file)), istreambuf_iterator<char>());
        file.close();

        content.erase(remove(content.begin(), content.end(), '\n'), content.end());
        content.erase(remove(content.begin(), content.end(), '\r'), content.end());

        return content;
    } catch (const fs::filesystem_error &e) {
        throw runtime_error("Filesystem error: " + string(e.what()));
    } catch (const exception &e) {
        throw runtime_error("Error getting current database: " + string(e.what()));
    }
}

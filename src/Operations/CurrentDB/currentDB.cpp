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
    string path = fs::current_path().string() + "/crrtdb/crrtdb.txt";

    ifstream file(path);
    if (!file.is_open()) {
        throw runtime_error("Cannot open current database file: " + path);
    }

    string content((istreambuf_iterator<char>(file)), istreambuf_iterator<char>());
    file.close();

    if (content.empty()) {
        throw runtime_error("No database found");
    }

    content.erase(remove(content.begin(), content.end(), '\n'), content.end());
    content.erase(remove(content.begin(), content.end(), '\r'), content.end());

    return content;
}

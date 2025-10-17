#include "currentDB.h"
#include <fstream>
#include <iostream>
#include <filesystem>
#include <algorithm>

namespace fs = std::filesystem;

std::string CurrentDB::get() {
    std::string path = fs::current_path().string() + "/crrtdb/crrtdb.txt";

    std::ifstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open current database file: " + path);
    }

    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    file.close();

    if (content.empty()) {
        throw std::runtime_error("No database found");
    }

    content.erase(std::remove(content.begin(), content.end(), '\n'), content.end());
    content.erase(std::remove(content.begin(), content.end(), '\r'), content.end());

    return content;
}

#include "createDatabase.h"
#include <iostream>

int main() {
    try {
        CreateDatabase::createDatabase("MashDB");

        std::vector<std::string> columns = {"id", "name", "age"};
        std::vector<std::string> dataTypes = {"INT", "TEXT", "INT"};
        std::vector<bool> isUnique = {true, false, false};
        std::vector<bool> notNull = {true, true, false};

        CreateDatabase::createTable("Users", columns, dataTypes, isUnique, notNull);
    }
    catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    return 0;
}
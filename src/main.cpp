#include "Parser/parser.h"
#include <iostream>
#include <string>

int main() {
    std::cout << "🧠 MashDB Interactive Console\n";
    std::cout << "Type your SQL-like commands (type 'exit;' to quit)\n\n";

    std::string query;

    while (true) {
        std::cout << "mashdb> ";
        std::getline(std::cin, query);

        // Trim spaces
        while (!query.empty() && isspace(query.back()))
            query.pop_back();

        if (query.empty())
            continue;

        // Exit command
        if (query == "exit;" || query == "EXIT;") {
            std::cout << "👋 Exiting MashDB console.\n";
            break;
        }

        try {
            ParseQuery::parse(query);
        } catch (const std::exception &e) {
            std::cerr << "❌ Error: " << e.what() << std::endl;
        }
    }

    return 0;
}

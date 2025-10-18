#include "Parser/parser.h"
#include <iostream>
#include <string>

using namespace std;

int main() {
    cout << "MashDB Interactive Console\n";
    cout << "Type your SQL-like commands (type 'exit;' to quit)\n\n";

    string query;

    while (true) {
        cout << "mashdb> ";
        getline(cin, query);

        while (!query.empty() && isspace(query.back()))
            query.pop_back();

        if (query.empty())
            continue;

        if (query == "exit;" || query == "EXIT;") {
            cout << "👋 Exiting MashDB console.\n";
            break;
        }

        try {
            ParseQuery::parse(query);
        } catch (const exception &e) {
            cerr << "❌ Error: " << e.what() << endl;
        }
    }

    return 0;
}

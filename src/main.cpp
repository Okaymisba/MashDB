#include "Parser/parser.h"
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>

using namespace std;

void runInteractive() {
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

        if (query == "exit" || query == "EXIT") {
            cout << "Exiting MashDB console.\n";
            break;
        }

        try {
            ParseQuery::parse(query);
        } catch (const exception &e) {
            cerr << "Error: " << e.what() << endl;
        }
    }
}

bool g_outputJson = false;

int main(int argc, char *argv[]) {
    vector<string> args(argv + 1, argv + argc);

    auto jsonIt = find(args.begin(), args.end(), "--json");
    if (jsonIt != args.end()) {
        g_outputJson = true;
        args.erase(jsonIt);
    }

    if (!args.empty()) {
        string query;
        for (size_t i = 0; i < args.size(); ++i) {
            if (i > 0) query += " ";
            query += args[i];
        }

        try {
            ParseQuery::parse(query);
        } catch (const exception &e) {
            if (g_outputJson) {
                cout << "{\"status\":\"error\",\"message\":\"" << e.what() << "\"}" << endl;
            } else {
                cerr << "Error: " << e.what() << endl;
            }
            return 1;
        }
    } else {
        runInteractive();
    }
    return 0;
}

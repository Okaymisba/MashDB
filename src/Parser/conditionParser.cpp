#include "conditionParser.h"
#include <regex>
#include <stdexcept>
#include <cctype>
#include <algorithm>
#include <nlohmann/json.hpp>

using namespace std;
using json = nlohmann::json;

Condition ConditionParser::parseCondition(const string &condition) {
    if (condition.empty()) {
        throw runtime_error("Empty condition");
    }

    // This regex matches: column operator value
    // Supports: =, !=, >, <, >=, <=, LIKE
    // Value can be: 'string' or number
    regex comparisonRegex(
        R"(^\s*(\w+)\s*(=|!=|>|<|>=|<=|\s+LIKE\s+)\s*('([^']*)'|(\d+(?:\.\d+)?)|(\w+))\s*$)",
        regex_constants::icase
    );

    smatch match;
    if (!regex_match(condition, match, comparisonRegex)) {
        throw runtime_error("Invalid condition format. Expected: column operator value");
    }

    Condition cond;
    cond.column = match[1].str();
    cond.op = trim(match[2].str());

    // Extract the value (could be in match[3], match[4], or match[5])
    if (match[4].matched) {
        // Number
        cond.value = match[4].str();
    } else if (match[5].matched) {
        // Unquoted word (for NULL, true, false)
        cond.value = match[5].str();
    } else {
        // Quoted string (remove the quotes)
        cond.value = match[3].str();
    }

    // Convert operator to lowercase for consistent comparison
    cond.op = toLower(cond.op);

    return cond;
}

bool ConditionParser::evaluateCondition(const json &value, const Condition &condition) {
    string fieldValue;

    // Handle different JSON types
    if (value.is_string()) {
        fieldValue = value.get<string>();
    } else if (value.is_number_integer()) {
        fieldValue = to_string(value.get<int64_t>());
    } else if (value.is_number_float()) {
        fieldValue = to_string(value.get<double>());
        // Remove trailing zeros and possible decimal point if not needed
        fieldValue.erase(fieldValue.find_last_not_of('0') + 1, string::npos);
        if (fieldValue.back() == '.') {
            fieldValue.pop_back();
        }
    } else if (value.is_boolean()) {
        fieldValue = value.get<bool>() ? "true" : "false";
    } else if (value.is_null()) {
        fieldValue = "NULL";
    } else if (value.is_object() || value.is_array()) {
        // For objects or arrays, convert to string representation
        fieldValue = value.dump();
    } else {
        return false; // Unsupported type
    }

    string op = toLower(condition.op);
    string condValue = condition.value;

    // Remove surrounding quotes from condition value if it's a string
    if (condValue.size() >= 2 && condValue.front() == '\'' && condValue.back() == '\'') {
        condValue = condValue.substr(1, condValue.size() - 2);
    }

    // Handle the comparison based on the operator
    if (op == "=") {
        return compareEqual(fieldValue, condValue);
    } else if (op == "!=") {
        return compareNotEqual(fieldValue, condValue);
    } else if (op == ">") {
        return compareGreaterThan(fieldValue, condValue);
    } else if (op == "<") {
        return compareLessThan(fieldValue, condValue);
    } else if (op == ">=") {
        return compareGreaterEqual(fieldValue, condValue);
    } else if (op == "<=") {
        return compareLessEqual(fieldValue, condValue);
    } else if (op == "like") {
        return compareLike(fieldValue, condValue);
    }

    throw runtime_error("Unsupported operator: " + op);
}

// For backward compatibility
bool ConditionParser::evaluate(const string &condition, const json &value) {
    Condition cond = parseCondition(condition);
    return evaluateCondition(value, cond);
}

bool ConditionParser::compareEqual(const string &fieldValue, const string &conditionValue) {
    // Handle NULL comparison
    if (toLower(conditionValue) == "null") {
        return fieldValue == "NULL" || fieldValue.empty();
    }
    return fieldValue == conditionValue;
}

bool ConditionParser::compareNotEqual(const string &fieldValue, const string &conditionValue) {
    return !compareEqual(fieldValue, conditionValue);
}

bool ConditionParser::compareGreaterThan(const string &fieldValue, const string &conditionValue) {
    try {
        if (fieldValue.find('.') != string::npos || conditionValue.find('.') != string::npos) {
            return stod(fieldValue) > stod(conditionValue);
        }
        return stoll(fieldValue) > stoll(conditionValue);
    } catch (...) {
        return fieldValue > conditionValue; // Fallback to string comparison
    }
}

bool ConditionParser::compareLessThan(const string &fieldValue, const string &conditionValue) {
    try {
        if (fieldValue.find('.') != string::npos || conditionValue.find('.') != string::npos) {
            return stod(fieldValue) < stod(conditionValue);
        }
        return stoll(fieldValue) < stoll(conditionValue);
    } catch (...) {
        return fieldValue < conditionValue; // Fallback to string comparison
    }
}

bool ConditionParser::compareGreaterEqual(const string &fieldValue, const string &conditionValue) {
    return compareEqual(fieldValue, conditionValue) || compareGreaterThan(fieldValue, conditionValue);
}

bool ConditionParser::compareLessEqual(const string &fieldValue, const string &conditionValue) {
    return compareEqual(fieldValue, conditionValue) || compareLessThan(fieldValue, conditionValue);
}

bool ConditionParser::compareLike(const string &fieldValue, const string &pattern) {
    // Convert SQL LIKE pattern to regex
    string regexPattern;
    for (char c: pattern) {
        if (c == '%') {
            regexPattern += ".*";
        } else if (c == '_') {
            regexPattern += ".";
        } else if (c == '.' || c == '^' || c == '$' || c == '|' || c == '(' ||
                   c == ')' || c == '[' || c == ']' || c == '{' || c == '}' ||
                   c == '\\' || c == '+' || c == '?') {
            regexPattern += "\\";
            regexPattern += c;
        } else {
            regexPattern += c;
        }
    }

    try {
        regex likeRegex(regexPattern, regex_constants::icase);
        return regex_match(fieldValue, likeRegex);
    } catch (const regex_error &) {
        return false; // Invalid regex pattern
    }
}

string ConditionParser::trim(const string &str) {
    auto start = str.begin();
    while (start != str.end() && isspace(*start)) {
        start++;
    }

    auto end = str.end();
    do {
        end--;
    } while (distance(start, end) > 0 && isspace(*end));

    return string(start, end + 1);
}

string ConditionParser::toLower(const string &str) {
    string result = str;
    transform(result.begin(), result.end(), result.begin(),
              [](unsigned char c) { return tolower(c); });
    return result;
}

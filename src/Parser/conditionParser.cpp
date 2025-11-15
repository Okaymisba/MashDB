#include "conditionParser.h"
#include <regex>
#include <stdexcept>
#include <cctype>
#include <algorithm>
#include <iostream>
#include <nlohmann/json.hpp>

using namespace std;
using json = nlohmann::json;

/**
 * @brief Parses a condition string into its components.
 *
 * The input string is expected to represent a condition in the format
 * "column operator value". Supported operators are: =, ==, !=, >, <, >=, <=, LIKE.
 * The value can be a number, a single-quoted string, a double-quoted string, or a word.
 *
 * @param condition The condition string to be parsed (e.g., "age >= 25" or "name LIKE 'John%'").
 * @return A Condition structure containing the parsed components:
 *         - column: The name of the column.
 *         - op: The comparison operator (e.g., "=", "LIKE").
 *         - value: The condition value (e.g., "25", "'John%'").
 * @throws std::runtime_error if the condition string is empty or has an invalid format.
 */
Condition ConditionParser::parseCondition(const string &condition) {
    if (condition.empty()) {
        throw runtime_error("Empty condition");
    }
    // This regex matches: column operator value
    // Supports: =, ==, !=, >, <, >=, <=, LIKE
    // Value can be: 'string', "string", or number
    regex comparisonRegex(
        R"(^\s*(\w+)\s*(={1,2}|!=|>|<|>=|<=|\s+LIKE\s+)\s*((?:'[^']*'|"[^"]*"|\d+(?:\.\d+)?|\w+))\s*$)",
        regex_constants::icase
    );

    smatch debug_match;
    if (!regex_match(condition, debug_match, comparisonRegex)) {
        cerr << "Debug - Failed to match condition: " << condition << endl;
    }

    smatch match;
    if (!regex_match(condition, match, comparisonRegex)) {
        throw runtime_error("Invalid condition format. Expected: column operator value");
    }

    Condition cond;
    cond.column = match[1].str();
    cond.op = trim(match[2].str());

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

    cond.op = toLower(cond.op);

    return cond;
}


/**
 * @brief Evaluates a condition for a given value
 *
 * @param value The value to evaluate the condition against
 * @param condition The condition to evaluate
 * @return true if the condition is true for the given value, false otherwise
 * @throws std::runtime_error if an unsupported operator is encountered
 */
bool ConditionParser::evaluateCondition(const json &value, const Condition &condition) {
    string fieldValue;

    if (value.is_string()) {
        fieldValue = value.get<string>();
    } else if (value.is_number_integer()) {
        fieldValue = to_string(value.get<int64_t>());
    } else if (value.is_number_float()) {
        fieldValue = to_string(value.get<double>());
        fieldValue.erase(fieldValue.find_last_not_of('0') + 1, string::npos);
        if (fieldValue.back() == '.') {
            fieldValue.pop_back();
        }
    } else if (value.is_boolean()) {
        fieldValue = value.get<bool>() ? "true" : "false";
    } else if (value.is_null()) {
        fieldValue = "NULL";
    } else if (value.is_object() || value.is_array()) {
        fieldValue = value.dump();
    } else {
        return false;
    }

    string op = toLower(condition.op);
    string condValue = condition.value;

    if (condValue.size() >= 2) {
        char quoteChar = condValue.front();
        if ((quoteChar == '\'' && condValue.back() == '\'') ||
            (quoteChar == '"' && condValue.back() == '"')) {
            condValue = condValue.substr(1, condValue.size() - 2);
        }
    }

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


/**
 * @brief Evaluates a condition string against a given JSON value.
 *
 * @param condition The condition string to be evaluated (e.g., "age > 25").
 * @param value The JSON value against which the condition is evaluated.
 * @return true if the condition is true for the given value, false otherwise.
 *
 * This function first parses the condition string into its components using the
 * `parseCondition` function, and then evaluates the condition against the given
 * value using the `evaluateCondition` function.
 */
bool ConditionParser::evaluate(const string &condition, const json &value) {
    Condition cond = parseCondition(condition);
    return evaluateCondition(value, cond);
}

/**
 * @brief Compares two strings for equality after trimming whitespace.
 *
 * @param fieldValue The field value to compare.
 * @param conditionValue The condition value to compare.
 * @return true if the trimmed strings are equal, false otherwise.
 *
 * This function first trims whitespace from both strings, and then compares them for
 * equality. If the condition value is "null", it returns true if the field value is
 * either "NULL" or empty.
 */
bool ConditionParser::compareEqual(const string &fieldValue, const string &conditionValue) {
    if (toLower(conditionValue) == "null") {
        return fieldValue == "NULL" || fieldValue.empty();
    }

    string trimmedField = trim(fieldValue);
    string trimmedCondition = trim(conditionValue);

    bool result = (trimmedField == trimmedCondition);

    return result;
}

/**
 * @brief Compares two strings for inequality after trimming whitespace.
 *
 * @param fieldValue The field value to compare.
 * @param conditionValue The condition value to compare.
 * @return true if the trimmed strings are not equal, false otherwise.
 *
 * This function first trims whitespace from both strings, and then compares them for
 * inequality. If the condition value is "null", it returns true if the field value is
 * not "NULL" and not empty.
 */
bool ConditionParser::compareNotEqual(const string &fieldValue, const string &conditionValue) {
    return !compareEqual(fieldValue, conditionValue);
}

/**
 * @brief Compares two strings for greater than after trimming whitespace.
 *
 * @param fieldValue The field value to compare.
 * @param conditionValue The condition value to compare.
 * @return true if the trimmed strings are greater than, false otherwise.
 *
 * This function first trims whitespace from both strings, and then compares them for
 * greater than. If the condition value is a floating point number, it uses the
 * `stod` function to compare the values. If the condition value is an integer,
 * it uses the `stoll` function to compare the values. If either string cannot be
 * converted to a number, it compares the strings lexicographically.
 */
bool ConditionParser::compareGreaterThan(const string &fieldValue, const string &conditionValue) {
    try {
        if (fieldValue.find('.') != string::npos || conditionValue.find('.') != string::npos) {
            return stod(fieldValue) > stod(conditionValue);
        }
        return stoll(fieldValue) > stoll(conditionValue);
    } catch (...) {
        return fieldValue > conditionValue;
    }
}

/**
 * @brief Compares two strings for less than after trimming whitespace.
 *
 * @param fieldValue The field value to compare.
 * @param conditionValue The condition value to compare.
 * @return true if the trimmed strings are less than, false otherwise.
 *
 * This function first trims whitespace from both strings, and then compares them for
 * less than. If the condition value is a floating point number, it uses the
 * `stod` function to compare the values. If the condition value is an integer,
 * it uses the `stoll` function to compare the values. If either string cannot be
 * converted to a number, it compares the strings lexicographically.
 */
bool ConditionParser::compareLessThan(const string &fieldValue, const string &conditionValue) {
    try {
        if (fieldValue.find('.') != string::npos || conditionValue.find('.') != string::npos) {
            return stod(fieldValue) < stod(conditionValue);
        }
        return stoll(fieldValue) < stoll(conditionValue);
    } catch (...) {
        return fieldValue < conditionValue;
    }
}

/**
 * @brief Compares two strings for greater than or equal after trimming whitespace.
 *
 * @param fieldValue The field value to compare.
 * @param conditionValue The condition value to compare.
 * @return true if the trimmed strings are greater than or equal, false otherwise.
 *
 * This function first trims whitespace from both strings, and then compares them for
 * greater than or equal. It returns true if the condition value is greater than or
 * equal to the field value, and false otherwise.
 */
bool ConditionParser::compareGreaterEqual(const string &fieldValue, const string &conditionValue) {
    return compareEqual(fieldValue, conditionValue) || compareGreaterThan(fieldValue, conditionValue);
}

/**
 * @brief Compares two strings for less than or equal after trimming whitespace.
 *
 * @param fieldValue The field value to compare.
 * @param conditionValue The condition value to compare.
 * @return true if the trimmed strings are less than or equal, false otherwise.
 *
 * This function first trims whitespace from both strings, and then compares them for
 * less than or equal. It returns true if the condition value is less than or equal to
 * the field value, and false otherwise.
 */
bool ConditionParser::compareLessEqual(const string &fieldValue, const string &conditionValue) {
    return compareEqual(fieldValue, conditionValue) || compareLessThan(fieldValue, conditionValue);
}

/**
 * @brief Compares two strings using a LIKE pattern after trimming whitespace.
 *
 * @param fieldValue The field value to compare.
 * @param pattern The LIKE pattern to compare against.
 * @return true if the trimmed field value matches the LIKE pattern, false otherwise.
 *
 * This function first trims whitespace from both strings, and then converts the
 * pattern to a regular expression pattern. It then uses the `regex_match` function
 * to compare the trimmed field value against the regular expression pattern.
 * If the regular expression pattern is invalid, it returns false.
 */
bool ConditionParser::compareLike(const string &fieldValue, const string &pattern) {
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
        return false;
    }
}

/**
 * @brief Trims whitespace from both sides of a string.
 *
 * @param str The string to trim.
 * @return A new string with whitespace removed from both sides.
 *
 * This function iterates through the input string from both ends and removes
 * whitespace characters until it reaches a non-whitespace character.
 */
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

/**
 * @brief Converts a string to lowercase.
 *
 * @param str The string to convert to lowercase.
 * @return A new string with all characters converted to lowercase.
 *
 * This function uses the `std::transform` algorithm to apply the `std::tolower`
 * function to each character in the input string.
 */
string ConditionParser::toLower(const string &str) {
    string result = str;
    transform(result.begin(), result.end(), result.begin(),
              [](unsigned char c) { return tolower(c); });
    return result;
}

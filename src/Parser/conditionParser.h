#ifndef MASHDB_CONDITION_PARSER_H
#define MASHDB_CONDITION_PARSER_H

#include <string>
#include <nlohmann/json.hpp>
#include <optional>

using json = nlohmann::json;

struct Condition {
    std::string column;
    std::string op; // =, !=, >, <, >=, <=, LIKE
    std::string value;
};

class ConditionParser {
public:
    /**
     * @brief Parses a condition string into its components
     *
     * @param condition The condition string (e.g., "name = 'John'")
     * @return Condition struct with parsed components
     * @throws std::runtime_error if the condition is invalid
     */
    static Condition parseCondition(const std::string &condition);

    /**
     * @brief Evaluates a value against a parsed condition
     *
     * @param value The value to evaluate
     * @param condition The parsed condition
     * @return true if the condition is true for the given value
     */
    static bool evaluateCondition(const json &value, const Condition &condition);

private:
    // Helper functions for different comparison operators
    static bool compareEqual(const std::string &fieldValue, const std::string &conditionValue);

    static bool compareNotEqual(const std::string &fieldValue, const std::string &conditionValue);

    static bool compareGreaterThan(const std::string &fieldValue, const std::string &conditionValue);

    static bool compareLessThan(const std::string &fieldValue, const std::string &conditionValue);

    static bool compareGreaterEqual(const std::string &fieldValue, const std::string &conditionValue);

    static bool compareLessEqual(const std::string &fieldValue, const std::string &conditionValue);

    static bool compareLike(const std::string &fieldValue, const std::string &pattern);

    // Helper to trim whitespace
    static std::string trim(const std::string &str);

    // Helper to convert string to lowercase
    static std::string toLower(const std::string &str);

    // For backward compatibility
    static bool evaluate(const std::string &condition, const json &value);
};

#endif // MASHDB_CONDITION_PARSER_H

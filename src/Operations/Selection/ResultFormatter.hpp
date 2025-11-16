#pragma once

#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include <iomanip>
#include <sstream>

using json = nlohmann::json;

namespace Selection {
    class ResultFormatter {
    public:
        /**
         * @brief Formats the query results in a tabular format
         *
         * @param result JSON array of result rows
         * @param columns List of columns to display (if empty, all columns are shown)
         * @return std::string Formatted table as a string
         */
        static std::string formatAsTable(const json &result, const std::vector<std::string> &columns = {});

        /**
         * @brief Returns the query results as a JSON string
         * @param result JSON array of result rows
         * @param columns List of columns to include (if empty, all columns are included)
         * @return std::string JSON string containing the results
         */
        static std::string formatAsJson(const json &result, const std::vector<std::string> &columns = {});

    private:
        /**
         * @brief Calculates the maximum width needed for each column
         */
        static std::vector<size_t> calculateColumnWidths(
            const json &result,
            const std::vector<std::string> &columns
        );

        /**
         * @brief Creates a horizontal line for the table
         */
        static std::string createHorizontalLine(const std::vector<size_t> &columnWidths);

        /**
         * @brief Formats a single row of data
         */
        static std::string formatRow(
            const json &row,
            const std::vector<std::string> &columns,
            const std::vector<size_t> &columnWidths
        );

        /**
         * @brief Converts any value to a string with proper formatting
         */
        template<typename T>
        static std::string valueToString(const T &value) {
            if (value.is_string()) {
                return value.template get<std::string>();
            } else if (value.is_number_integer()) {
                return std::to_string(value.template get<int64_t>());
            } else if (value.is_number_float()) {
                std::ostringstream oss;
                double val = value.template get<double>();
                oss << std::fixed << std::setprecision(2) << val;
                std::string str = oss.str();
                str.erase(str.find_last_not_of('0') + 1, std::string::npos);
                if (str.back() == '.') {
                    str.pop_back();
                }
                return str;
            } else if (value.is_boolean()) {
                return value.template get<bool>() ? "true" : "false";
            } else if (value.is_null()) {
                return "NULL";
            }
            return "";
        }
    };
}

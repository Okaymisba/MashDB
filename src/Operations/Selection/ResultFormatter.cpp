#include "ResultFormatter.hpp"
#include <algorithm>
#include <iomanip>
#include <sstream>
#include <nlohmann/json.hpp>

namespace Selection {
    std::string ResultFormatter::formatAsTable(const json &result, const std::vector<std::string> &columns) {
        if (result.empty()) {
            return "No rows returned\n";
        }

        std::vector<std::string> cols = columns;
        if (cols.empty() && !result.empty()) {
            for (auto &[key, _]: result[0].items()) {
                cols.push_back(key);
            }
        }

        std::vector<size_t> widths = calculateColumnWidths(result, cols);

        std::stringstream ss;
        ss << createHorizontalLine(widths) << "\n";

        for (size_t i = 0; i < cols.size(); ++i) {
            ss << "|" << std::left << std::setw(widths[i]) << std::setfill(' ')
                    << " " + cols[i] + " ";
        }
        ss << "|\n";

        ss << createHorizontalLine(widths) << "\n";

        for (const auto &row: result) {
            ss << formatRow(row, cols, widths) << "\n";
        }

        ss << createHorizontalLine(widths) << "\n";
        ss << result.size() << " row" << (result.size() != 1 ? "s" : "") << " in set\n";

        return ss.str();
    }

    std::vector<size_t> ResultFormatter::calculateColumnWidths(
        const json &result,
        const std::vector<std::string> &columns
    ) {
        std::vector<size_t> widths(columns.size(), 0);

        for (size_t i = 0; i < columns.size(); ++i) {
            widths[i] = columns[i].length() + 2;
        }
        for (const auto &row: result) {
            for (size_t i = 0; i < columns.size(); ++i) {
                const auto &col = columns[i];
                std::string value;

                if (row.contains(col)) {
                    value = valueToString(row[col]);
                } else {
                    value = "NULL";
                }

                widths[i] = std::max(widths[i], value.length() + 2);
            }
        }

        return widths;
    }

    std::string ResultFormatter::createHorizontalLine(const std::vector<size_t> &columnWidths) {
        std::string line = "+";
        for (size_t width: columnWidths) {
            line += std::string(width + 2, '-');
            line += "+";
        }
        return line;
    }

    std::string ResultFormatter::formatRow(
        const json &row,
        const std::vector<std::string> &columns,
        const std::vector<size_t> &columnWidths
    ) {
        std::stringstream ss;

        for (size_t i = 0; i < columns.size(); ++i) {
            const auto &col = columns[i];
            std::string value;

            if (row.contains(col)) {
                value = valueToString(row[col]);
            } else {
                value = "NULL";
            }

            ss << "|" << std::left << std::setw(columnWidths[i]) << std::setfill(' ')
                    << " " + value + " ";
        }

        ss << "|";
        return ss.str();
    }

    std::string ResultFormatter::formatAsJson(const json &result, const std::vector<std::string> &columns) {
        if (result.empty()) {
            return "{\"status\":\"success\",\"data\":[]}";
        }

        json output;
        output["status"] = "success";

        std::vector<std::string> cols = columns;
        if (cols.empty() && !result.empty()) {
            for (auto &[key, _]: result[0].items()) {
                cols.push_back(key);
            }
        }

        json filteredResults = json::array();
        for (const auto &row: result) {
            json filteredRow;
            for (const auto &col: cols) {
                if (row.contains(col)) {
                    filteredRow[col] = row[col];
                }
            }
            filteredResults.push_back(filteredRow);
        }

        output["data"] = filteredResults;
        output["count"] = result.size();

        return output.dump(4);
    }
}

#include <string>
#include <vector>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <memory>
#include <stdexcept>
#include <algorithm>
#include <sstream>
class JoinPlanGenerator {
private:
    std::unordered_map<std::string, std::string> aliasToTable;
    
    struct JoinInfo {
        std::vector<std::string> tables;
        std::vector<std::string> joinConditions;
        std::unordered_map<std::string, std::vector<std::string>> tableJoins;
    };

    void parseAliases(const std::string& query) {
        size_t fromPos = query.find("FROM");
        size_t wherePos = query.find("WHERE");
        if (fromPos == std::string::npos) return;
        
        std::string fromClause = query.substr(fromPos + 4, 
            wherePos != std::string::npos ? wherePos - (fromPos + 4) : std::string::npos);
        
        std::istringstream iss(fromClause);
        std::string token;
        std::string table;
        bool expectingAlias = false;
        
        while (iss >> token) {
            // Remove commas
            if (!token.empty() && token.back() == ',') {
                token.pop_back();
            }
            
            if (token == "AS") {
                expectingAlias = true;
                continue;
            }
            
            if (expectingAlias) {
                aliasToTable[token] = table;
                expectingAlias = false;
                table.clear();
            } else {
                table = token;
            }
        }
    }
    
    JoinInfo parseJoinConditions(const std::string& query) {
        JoinInfo info;
        size_t wherePos = query.find("WHERE");
        if (wherePos == std::string::npos) return info;
        
        std::string whereClause = query.substr(wherePos + 5);
        std::vector<std::string> conditions;
        
        // Split by AND, properly handling whitespace
        size_t start = 0;
        while (start < whereClause.length()) {
            size_t andPos = whereClause.find("AND", start);
            if (andPos == std::string::npos) {
                conditions.push_back(trim(whereClause.substr(start)));
                break;
            }
            
            std::string condition = trim(whereClause.substr(start, andPos - start));
            if (!condition.empty()) {
                conditions.push_back(condition);
            }
            start = andPos + 3;
        }
        
        // Process each condition
        for (const auto& condition : conditions) {
            if (condition.find('=') != std::string::npos && condition.find('.') != std::string::npos) {
                auto tables = extractTablesFromJoin(condition);
                if (!tables.first.empty() && !tables.second.empty()) {
                    // Convert aliases to table names
                    std::string table1 = aliasToTable[tables.first];
                    std::string table2 = aliasToTable[tables.second];
                    
                    info.joinConditions.push_back(condition);
                    info.tableJoins[table1].push_back(table2);
                    info.tableJoins[table2].push_back(table1);
                    
                    if (std::find(info.tables.begin(), info.tables.end(), table1) == info.tables.end()) {
                        info.tables.push_back(table1);
                    }
                    if (std::find(info.tables.begin(), info.tables.end(), table2) == info.tables.end()) {
                        info.tables.push_back(table2);
                    }
                }
            }
        }
        return info;
    }
    
    std::pair<std::string, std::string> extractTablesFromJoin(const std::string& condition) {
        size_t eqPos = condition.find('=');
        if (eqPos == std::string::npos) return {"", ""};
        
        std::string left = trim(condition.substr(0, eqPos));
        std::string right = trim(condition.substr(eqPos + 1));
        
        size_t dotPosLeft = left.find('.');
        size_t dotPosRight = right.find('.');
        
        if (dotPosLeft == std::string::npos || dotPosRight == std::string::npos) 
            return {"", ""};
            
        return {
            trim(left.substr(0, dotPosLeft)),
            trim(right.substr(0, dotPosRight))
        };
    }
    
    std::string trim(const std::string& str) {
        size_t first = str.find_first_not_of(" \t\n\r");
        if (first == std::string::npos) return "";
        size_t last = str.find_last_not_of(" \t\n\r");
        return str.substr(first, last - first + 1);
    }
    
    std::string generateOptimalPlan(const JoinInfo& info) {
        if (info.tables.empty()) return "";
        
        std::unordered_set<std::string> used;
        std::string plan = info.tables[0];
        used.insert(info.tables[0]);
        
        while (used.size() < info.tables.size()) {
            std::string bestTable;
            int maxConnections = -1;
            
            for (const auto& table : info.tables) {
                if (used.find(table) != used.end()) continue;
                
                int connections = 0;
                for (const auto& joinTable : info.tableJoins.at(table)) {
                    if (used.find(joinTable) != used.end()) {
                        connections++;
                    }
                }
                
                if (connections > maxConnections) {
                    maxConnections = connections;
                    bestTable = table;
                }
            }
            
            if (bestTable.empty()) break;
            plan = "(" + plan + " ⨝ " + bestTable + ")";
            used.insert(bestTable);
        }
        
        return plan;
    }
    std::string generatePostgresStylePlan(const JoinInfo& info) {
        if (info.tables.empty()) return "";
        
        // Calculate join counts for each table
        std::unordered_map<std::string, int> joinCount;
        for (const auto& [table, joins] : info.tableJoins) {
            joinCount[table] = joins.size();
        }
        
        // Start with the table that has the most joins
        std::string mostJoinedTable;
        int maxJoins = -1;
        for (const auto& [table, count] : joinCount) {
            if (count > maxJoins) {
                maxJoins = count;
                mostJoinedTable = table;
            }
        }
        
        // Build the join plan from inside out
        std::unordered_set<std::string> used;
        std::string plan = mostJoinedTable;
        used.insert(mostJoinedTable);
        
        while (used.size() < info.tables.size()) {
            std::string nextTable;
            int maxConnections = -1;
            
            // Find table with most connections to already used tables
            for (const auto& table : info.tables) {
                if (used.find(table) != used.end()) continue;
                
                int connections = 0;
                for (const auto& joinTable : info.tableJoins.at(table)) {
                    if (used.find(joinTable) != used.end()) {
                        connections++;
                    }
                }
                
                // Prefer tables with higher join count when connections are equal
                if (connections > maxConnections || 
                    (connections == maxConnections && joinCount[table] > joinCount[nextTable])) {
                    maxConnections = connections;
                    nextTable = table;
                }
            }
            
            if (nextTable.empty()) break;
            
            // Wrap previous plan in parentheses and add new table
            plan = "(" + nextTable + " ⨝ " + plan + ")";
            used.insert(nextTable);
        }
        
        return plan;
    }
public:
    std::string getOptimalJoinPlan(const std::string& query) {
        aliasToTable.clear();
        parseAliases(query);
        JoinInfo joinInfo = parseJoinConditions(query);
        return generatePostgresStylePlan(joinInfo);
    }
};

std::string getOptimalPlan(const std::string& input_query) {
    JoinPlanGenerator generator;
    return generator.getOptimalJoinPlan(input_query);
}
int main() {
    std::string input_query = R"(
   SELECT MIN(mi_idx.info) AS rating,
       MIN(t.title) AS movie_title
FROM info_type AS it,
     keyword AS k,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     title AS t
WHERE it.info ='rating'
  AND k.keyword LIKE '%sequel%'
  AND mi_idx.info > '2.0'
  AND t.production_year > 1990
  AND t.id = mi_idx.movie_id
  AND t.id = mk.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND k.id = mk.keyword_id
  AND it.id = mi_idx.info_type_id;
    )";

    try {
        std::string optimalPlan = getOptimalPlan(input_query);
        std::cout << "Optimal Join Plan: " << optimalPlan << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
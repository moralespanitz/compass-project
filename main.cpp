#include <libpq-fe.h>
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
#include <random>
#include <cmath>

// FastAGM Sketch implementation
class FastAGMSketch {
private:
    static const int HASH_FUNCTIONS = 5;
    static const int SKETCH_SIZE = 1000;
    std::vector<std::vector<double>> sketch;
    std::vector<std::vector<int>> hashSeeds;
    
    int hash(int value, int seed) const {
        std::hash<int> hasher;
        return (hasher(value ^ seed) % SKETCH_SIZE + SKETCH_SIZE) % SKETCH_SIZE;
    }

public:
    FastAGMSketch() : sketch(HASH_FUNCTIONS, std::vector<double>(SKETCH_SIZE, 0)) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(1, INT_MAX);
        
        hashSeeds.resize(HASH_FUNCTIONS);
        for (int i = 0; i < HASH_FUNCTIONS; i++) {
            hashSeeds[i].resize(2);
            hashSeeds[i][0] = dis(gen);
            hashSeeds[i][1] = dis(gen);
        }
    }

    void update(int vertex1, int vertex2, double weight = 1.0) {
        for (int i = 0; i < HASH_FUNCTIONS; i++) {
            int h1 = hash(vertex1, hashSeeds[i][0]);
            int h2 = hash(vertex2, hashSeeds[i][1]);
            sketch[i][h1 ^ h2] += weight;
        }
    }

    double estimate(int vertex1, int vertex2) const {
        std::vector<double> estimates;
        for (int i = 0; i < HASH_FUNCTIONS; i++) {
            int h1 = hash(vertex1, hashSeeds[i][0]);
            int h2 = hash(vertex2, hashSeeds[i][1]);
            estimates.push_back(sketch[i][h1 ^ h2]);
        }
        
        std::sort(estimates.begin(), estimates.end());
        return estimates[HASH_FUNCTIONS / 2];
    }
};

class JoinPlanGenerator {
private:
    std::unordered_map<std::string, std::string> aliasToTable;
    PGconn* dbConn;
    FastAGMSketch agmSketch;
    
    struct JoinInfo {
        std::vector<std::string> tables;
        std::vector<std::string> joinConditions;
        std::unordered_map<std::string, std::vector<std::string>> tableJoins;
        std::unordered_map<std::string, double> tableCardinalities;
    };

    std::string trim(const std::string& str) {
        size_t first = str.find_first_not_of(" \t\n\r");
        if (first == std::string::npos) return "";
        size_t last = str.find_last_not_of(" \t\n\r");
        return str.substr(first, last - first + 1);
    }

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

    JoinInfo parseJoinConditions(const std::string& query) {
        JoinInfo info;
        size_t wherePos = query.find("WHERE");
        if (wherePos == std::string::npos) return info;
        
        std::string whereClause = query.substr(wherePos + 5);
        std::vector<std::string> conditions;
        
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
        
        for (const auto& condition : conditions) {
            if (condition.find('=') != std::string::npos && condition.find('.') != std::string::npos) {
                auto tables = extractTablesFromJoin(condition);
                if (!tables.first.empty() && !tables.second.empty()) {
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
    
    double getTableCardinality(const std::string& tableName) {
        std::string query = "SELECT reltuples::bigint FROM pg_class WHERE relname = $1;";
        const char* paramValues[1] = {tableName.c_str()};
        
        PGresult* res = PQexecParams(dbConn, query.c_str(), 1, nullptr, paramValues, 
                                   nullptr, nullptr, 0);
        
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            std::string error = PQerrorMessage(dbConn);
            PQclear(res);
            throw std::runtime_error("Failed to get table cardinality: " + error);
        }
        
        double cardinality = std::stod(PQgetvalue(res, 0, 0));
        PQclear(res);
        return cardinality;
    }
    
    void updateSketchWithJoinStatistics(JoinInfo& info) {
        for (const auto& [table1, joins] : info.tableJoins) {
            for (const auto& table2 : joins) {
                std::string query = "SELECT tablename, attname, n_distinct, correlation "
                                  "FROM pg_stats WHERE tablename IN ($1, $2);";
                                  
                const char* paramValues[2] = {table1.c_str(), table2.c_str()};
                PGresult* res = PQexecParams(dbConn, query.c_str(), 2, nullptr, 
                                           paramValues, nullptr, nullptr, 0);
                
                if (PQresultStatus(res) != PGRES_TUPLES_OK) {
                    PQclear(res);
                    continue;
                }
                
                double selectivity = calculateJoinSelectivity(res);
                int t1Hash = std::hash<std::string>{}(table1);
                int t2Hash = std::hash<std::string>{}(table2);
                agmSketch.update(t1Hash, t2Hash, selectivity);
                
                PQclear(res);
            }
        }
    }
    
    double calculateJoinSelectivity(PGresult* stats) {
        double selectivity = 1.0;
        for (int i = 0; i < PQntuples(stats); i++) {
            double n_distinct = std::stod(PQgetvalue(stats, i, 2));
            if (n_distinct > 0) {
                selectivity *= (1.0 / n_distinct);
            }
        }
        return selectivity;
    }
    
    std::string generatePostgresStylePlan(JoinInfo& info) {
        if (info.tables.empty()) return "";
        
        std::unordered_map<std::string, double> tableScores;
        for (const auto& table : info.tables) {
            double score = 0;
            int t1Hash = std::hash<std::string>{}(table);
            
            for (const auto& joinTable : info.tableJoins.at(table)) {
                int t2Hash = std::hash<std::string>{}(joinTable);
                double joinScore = agmSketch.estimate(t1Hash, t2Hash);
                score += joinScore;
            }
            
            if (info.tableCardinalities.count(table) > 0) {
                score *= (1.0 / std::log1p(info.tableCardinalities.at(table)));
            }
            tableScores[table] = score;
        }
        
        std::unordered_set<std::string> used;
        std::string plan;
        
        auto bestStart = std::max_element(tableScores.begin(), tableScores.end(),
            [](const auto& p1, const auto& p2) { return p1.second < p2.second; });
            
        plan = bestStart->first;
        used.insert(bestStart->first);
        
        while (used.size() < info.tables.size()) {
            std::string nextTable;
            double bestScore = -1;
            
            for (const auto& table : info.tables) {
                if (used.find(table) != used.end()) continue;
                
                double score = tableScores[table];
                for (const auto& usedTable : used) {
                    int t1Hash = std::hash<std::string>{}(table);
                    int t2Hash = std::hash<std::string>{}(usedTable);
                    score *= (1 + agmSketch.estimate(t1Hash, t2Hash));
                }
                
                if (score > bestScore) {
                    bestScore = score;
                    nextTable = table;
                }
            }
            
            if (nextTable.empty()) break;
            plan = "(" + nextTable + " ⨝ " + plan + ")";
            used.insert(nextTable);
        }
        
        return plan;
    }
    
public:
    JoinPlanGenerator(const char* conninfo) {
        dbConn = PQconnectdb(conninfo);
        if (PQstatus(dbConn) != CONNECTION_OK) {
            std::string error = PQerrorMessage(dbConn);
            throw std::runtime_error("Database connection failed: " + error);
        }
    }
    
    ~JoinPlanGenerator() {
        if (dbConn) {
            PQfinish(dbConn);
        }
    }
    
    std::string getOptimalJoinPlan(const std::string& query) {
        aliasToTable.clear();
        parseAliases(query);
        JoinInfo joinInfo = parseJoinConditions(query);
        
        for (const auto& table : joinInfo.tables) {
            joinInfo.tableCardinalities[table] = getTableCardinality(table);
        }
        
        updateSketchWithJoinStatistics(joinInfo);
        
        return generatePostgresStylePlan(joinInfo);
    }
};

int main() {
    const char *conninfo = "dbname=job user=postgres password=postgres hostaddr=127.0.0.1 port=5432";
    
    try {
        JoinPlanGenerator generator(conninfo);
    
        std::string input_query = R"SQL(
        SELECT MIN(k.keyword) AS movie_keyword,
       MIN(n.name) AS actor_name,
       MIN(t.title) AS marvel_movie
FROM cast_info AS ci,
     keyword AS k,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE k.keyword = 'marvel-cinematic-universe'
  AND n.name LIKE '%Downey%Robert%'
  AND t.production_year > 2000
  AND k.id = mk.keyword_id
  AND t.id = mk.movie_id
  AND t.id = ci.movie_id
  AND ci.movie_id = mk.movie_id
  AND n.id = ci.person_id;
        )SQL";
        
        std::string optimalPlan = generator.getOptimalJoinPlan(input_query);
        std::cout << "Optimal Join Plan:\n" << optimalPlan << std::endl;
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
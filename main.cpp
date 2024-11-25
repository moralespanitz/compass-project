#include <iostream>
#include <libpq-fe.h>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <queue>
#include <sstream>
#include <random>
#include <algorithm>
#include <string>

// FastAGMSSketch class remains the same
class FastAGMSSketch {
private:
    std::vector<std::vector<int>> sketch;
    std::hash<int> hashFunc;
    int rows, cols;

    int hash(int value, int seed) const {
        std::mt19937 rng(seed);
        rng.seed(seed);
        return (value ^ rng()) % cols;
    }

public:
    FastAGMSSketch() : rows(0), cols(0), sketch(0, std::vector<int>(0, 0)) {}

    FastAGMSSketch(int rows, int cols) : rows(rows), cols(cols), sketch(rows, std::vector<int>(cols, 0)) {}

    void update(int value) {
        for (int i = 0; i < rows; ++i) {
            int col = hash(value, i);
            sketch[i][col] += (value > 0 ? 1 : -1);
        }
    }

    int dotProduct(const FastAGMSSketch &other) const {
        if (rows != other.rows || cols != other.cols) {
            throw std::invalid_argument("Sketch dimensions do not match.");
        }

        int result = 0;
        for (int i = 0; i < rows; ++i) {
            for (int j = 0; j < cols; ++j) {
                result += sketch[i][j] * other.sketch[i][j];
            }
        }
        return result;
    }

    FastAGMSSketch merge(const FastAGMSSketch &other) const {
        if (rows != other.rows || cols != other.cols) {
            throw std::invalid_argument("Sketch dimensions do not match.");
        }

        FastAGMSSketch merged(rows, cols);
        for (int i = 0; i < rows; ++i) {
            for (int j = 0; j < cols; ++j) {
                merged.sketch[i][j] = std::min(std::abs(sketch[i][j]), std::abs(other.sketch[i][j]));
            }
        }
        return merged;
    }
};

// Enhanced query analysis structure with alias support
struct TableAlias {
    std::string table;
    std::string alias;
};
struct QueryInfo {
    std::vector<std::string> tables;
    std::vector<std::string> joinConditions;
    std::vector<std::string> whereConditions;
    std::unordered_map<std::string, std::string> aliasToTable;  // Maps alias to actual table name
    std::unordered_map<std::string, std::string> tableToAlias;  // Maps actual table name to alias
};

QueryInfo analyzeQuery(const std::string& query, const std::unordered_map<std::string, std::string>& aliasMap) {
    QueryInfo info;
    std::string upperQuery = query;
    std::transform(upperQuery.begin(), upperQuery.end(), upperQuery.begin(), ::toupper);
    
    // Create reverse mapping
    for (const auto& [alias, table] : aliasMap) {
        std::string upperAlias = alias;
        std::string upperTable = table;
        std::transform(upperAlias.begin(), upperAlias.end(), upperAlias.begin(), ::toupper);
        std::transform(upperTable.begin(), upperTable.end(), upperTable.begin(), ::toupper);
        info.aliasToTable[upperAlias] = upperTable;
        info.tableToAlias[upperTable] = upperAlias;
    }
    
    // Extract tables
    size_t fromPos = upperQuery.find("FROM");
    size_t wherePos = upperQuery.find("WHERE");
    if (fromPos != std::string::npos) {
        std::string tableSection = upperQuery.substr(
            fromPos + 4, 
            wherePos != std::string::npos ? wherePos - (fromPos + 4) : std::string::npos
        );
        
        std::istringstream iss(tableSection);
        std::string token;
        while (iss >> token) {
            if (token != "," && token != "JOIN" && token != "AS") {
                token.erase(std::remove(token.begin(), token.end(), ','), token.end());
                if (!token.empty() && info.aliasToTable.find(token) != info.aliasToTable.end()) {
                    info.tables.push_back(info.aliasToTable[token]);
                }
            }
        }
    }

    // Extract join conditions
    if (wherePos != std::string::npos) {
        std::string whereSection = upperQuery.substr(wherePos + 5);
        size_t pos = 0;
        while ((pos = whereSection.find(" AND ")) != std::string::npos) {
            std::string condition = whereSection.substr(0, pos);
            condition.erase(0, condition.find_first_not_of(" \n\r\t"));
            condition.erase(condition.find_last_not_of(" \n\r\t") + 1);
            
            if (condition.find('=') != std::string::npos && 
                condition.find('.') != std::string::npos) {
                info.joinConditions.push_back(condition);
            } else {
                info.whereConditions.push_back(condition);
            }
            whereSection = whereSection.substr(pos + 5);
        }
        
        whereSection.erase(0, whereSection.find_first_not_of(" \n\r\t"));
        whereSection.erase(whereSection.find_last_not_of(" \n\r\t") + 1);
        if (!whereSection.empty()) {
            if (whereSection.find('=') != std::string::npos && 
                whereSection.find('.') != std::string::npos) {
                info.joinConditions.push_back(whereSection);
            } else {
                info.whereConditions.push_back(whereSection);
            }
        }
    }

    return info;
}

FastAGMSSketch buildSketchFromQuery(PGconn *conn, const std::string &tableName, int rows, int cols) {
    FastAGMSSketch sketch(rows, cols);
    std::string query = "SELECT id FROM " + tableName + " LIMIT 1000";
    
    PGresult *res = PQexec(conn, query.c_str());
    
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::cerr << "Query failed: " << PQerrorMessage(conn) << std::endl;
        PQclear(res);
        throw std::runtime_error("Failed to execute query.");
    }

    int nRows = PQntuples(res);
    for (int i = 0; i < nRows; ++i) {
        int value = std::stoi(PQgetvalue(res, i, 0));
        sketch.update(value);
    }

    PQclear(res);
    return sketch;
}

struct JoinGraph {
    std::unordered_map<std::string, FastAGMSSketch> tableSketches;
    std::vector<std::pair<std::string, std::string>> joins;
    std::unordered_map<std::string, std::vector<std::string>> joinConditions;
};

std::string buildJoinPlan(const JoinGraph &graph) {
    std::unordered_map<std::string, std::string> resultNames;
    std::unordered_set<std::string> joinedTables;
    std::unordered_map<std::string, std::unordered_set<std::string>> dependencies;

    auto cmp = [&graph](const std::pair<std::string, std::string> &a,
                        const std::pair<std::string, std::string> &b) {
        FastAGMSSketch mergedA = graph.tableSketches.at(a.first).merge(graph.tableSketches.at(a.second));
        FastAGMSSketch mergedB = graph.tableSketches.at(b.first).merge(graph.tableSketches.at(b.second));
        return mergedA.dotProduct(graph.tableSketches.at(a.first)) > 
               mergedB.dotProduct(graph.tableSketches.at(b.first));
    };

    std::priority_queue<std::pair<std::string, std::string>,
                        std::vector<std::pair<std::string, std::string>>,
                        decltype(cmp)>
        pq(cmp);

    for (const auto &join : graph.joins) {
        pq.push(join);
    }

    std::string joinPlan;
    while (!pq.empty()) {
        auto [tableA, tableB] = pq.top();
        pq.pop();

        // Skip if both tables are already in the same join group
        if (!dependencies[tableA].empty() && !dependencies[tableB].empty() &&
            dependencies[tableA] == dependencies[tableB]) {
            continue;
        }

        std::string left = resultNames.count(tableA) ? resultNames[tableA] : tableA;
        std::string right = resultNames.count(tableB) ? resultNames[tableB] : tableB;

        // Add join conditions if they exist
        std::string joinCond = "";
        std::string key = tableA + "_" + tableB;
        if (graph.joinConditions.count(key)) {
            joinCond = " [" + graph.joinConditions.at(key)[0] + "]";
        }

        std::string newResult = "(" + left + " ⨝" + joinCond + " " + right + ")";
        joinPlan = newResult;

        // Update dependencies
        std::unordered_set<std::string> newDeps = dependencies[tableA];
        newDeps.insert(dependencies[tableB].begin(), dependencies[tableB].end());
        newDeps.insert(tableA);
        newDeps.insert(tableB);
        
        for (const auto& table : newDeps) {
            dependencies[table] = newDeps;
            resultNames[table] = newResult;
        }

        joinedTables.insert(tableA);
        joinedTables.insert(tableB);
    }

    return joinPlan;
}

int main() {
    const char* conninfo = "dbname=job user=postgres password=postgres hostaddr=127.0.0.1 port=5432";
    PGconn *conn = PQconnectdb(conninfo);

    if (PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "Connection to database failed: " << PQerrorMessage(conn) << std::endl;
        PQfinish(conn);
        return 1;
    }
    std::cout << "Connected to PostgreSQL!" << std::endl;

    // Define the alias mapping (alias -> actual table name)
    std::unordered_map<std::string, std::string> aliasMap = {
        {"ak", "aka_name"},
        {"an", "aka_title"},
        {"cct", "cast_info"},
        {"ch", "char_name"},
        {"ci", "cast_info"},
        {"cn", "company_name"},
        {"ct", "company_type"},
        {"it", "info_type"},
        {"k", "keyword"},
        {"lt", "link_type"},
        {"mc", "movie_companies"},
        {"mi", "movie_info"},
        {"mi_idx", "movie_info_idx"},
        {"mk", "movie_keyword"},
        {"ml", "movie_link"},
        {"n", "name"},
        {"pi", "person_info"},
        {"rt", "role_type"},
        {"t", "title"}
    };

    // Define the example query similar to JOB benchmark format
    std::string query = R"(
        SELECT MIN(mc.note) AS production_note,
        MIN(t.title) AS movie_title,
        MIN(t.production_year) AS movie_year
    FROM company_type AS ct,
        info_type AS it,
        movie_companies AS mc,
        movie_info_idx AS mi_idx,
        title AS t
    WHERE ct.kind = 'production companies'
    AND it.info = 'top 250 rank'
    AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
    AND (mc.note LIKE '%(co-production)%'
        OR mc.note LIKE '%(presents)%')
    AND ct.id = mc.company_type_id
    AND t.id = mc.movie_id
    AND t.id = mi_idx.movie_id
    AND mc.movie_id = mi_idx.movie_id
    AND it.id = mi_idx.info_type_id;
    )";

    // Analyze the query with the alias mapping
    QueryInfo queryInfo = analyzeQuery(query, aliasMap);
    
    // Create the join graph
    JoinGraph graph;

    // Print table mappings for debugging
    std::cout << "\nTable Mappings:" << std::endl;
    for (const auto& [alias, table] : aliasMap) {
        std::cout << "Table: " << table << " -> Alias: " << alias << std::endl;
    }

    // Build sketches using actual table names
    for (const auto& table : queryInfo.tables) {
        try {
            std::string alias = queryInfo.tableToAlias[table];
            graph.tableSketches[alias] = buildSketchFromQuery(conn, table, 10, 50);
            std::cout << "Built sketch for " << table << " (alias: " << alias << ")" << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "Error building sketch for " << table << ": " << e.what() << std::endl;
            continue;
        }
    }

    // Add joins with proper alias mapping
    for (const auto& join : queryInfo.joinConditions) {
        std::istringstream iss(join);
        std::string leftPart, equals, rightPart;
        iss >> leftPart >> equals >> rightPart;
        
        if (equals == "=") {
            size_t dotPos1 = leftPart.find('.');
            size_t dotPos2 = rightPart.find('.');
            if (dotPos1 != std::string::npos && dotPos2 != std::string::npos) {
                std::string alias1 = leftPart.substr(0, dotPos1);
                std::string alias2 = rightPart.substr(0, dotPos2);
                
                if (graph.tableSketches.count(alias1) && graph.tableSketches.count(alias2)) {
                    graph.joins.push_back({alias1, alias2});
                    graph.joinConditions[alias1 + "_" + alias2] = {join};
                }
            }
        }
    }

    // Print extracted information
    std::cout << "\nExtracted Join Conditions:" << std::endl;
    for (const auto& join : queryInfo.joinConditions) {
        std::cout << "- " << join << std::endl;
    }

    // Build and print the join plan
    std::string joinPlan = buildJoinPlan(graph);
    std::cout << "\nOptimal Join Plan: " << joinPlan << std::endl;

    PQfinish(conn);
    return 0;
}
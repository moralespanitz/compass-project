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

// Enhanced query analysis structure
struct QueryInfo {
    std::vector<std::string> tables;
    std::vector<std::string> joinConditions;
    std::vector<std::string> whereConditions;
};

// Enhanced function to extract table names and join conditions
QueryInfo analyzeQuery(const std::string& query) {
    QueryInfo info;
    std::string upperQuery = query;
    std::transform(upperQuery.begin(), upperQuery.end(), upperQuery.begin(), ::toupper);
    
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
                // Remove "AS" aliases if present
                size_t asPos = token.find("AS");
                if (asPos != std::string::npos) {
                    token = token.substr(0, asPos);
                }
                // Trim whitespace
                token.erase(0, token.find_first_not_of(" \n\r\t"));
                token.erase(token.find_last_not_of(" \n\r\t") + 1);
                if (!token.empty()) {
                    info.tables.push_back(token);
                }
            }
        }
    }

    // Extract join and where conditions
    if (wherePos != std::string::npos) {
        std::string whereSection = upperQuery.substr(wherePos + 5);
        size_t pos = 0;
        while ((pos = whereSection.find(" AND ")) != std::string::npos) {
            std::string condition = whereSection.substr(0, pos);
            if (condition.find('=') != std::string::npos) {
                info.joinConditions.push_back(condition);
            } else {
                info.whereConditions.push_back(condition);
            }
            whereSection = whereSection.substr(pos + 5);  // Skip " AND "
        }
        // Handle the last condition
        if (whereSection.find('=') != std::string::npos) {
            info.joinConditions.push_back(whereSection);
        } else {
            info.whereConditions.push_back(whereSection);
        }
    }

    return info;
}

// Modified buildSketchFromQuery function
FastAGMSSketch buildSketchFromQuery(PGconn *conn, const std::string &query, int rows, int cols) {
    FastAGMSSketch sketch(rows, cols);
    
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

    auto cmp = [&graph](const std::pair<std::string, std::string> &a,
                        const std::pair<std::string, std::string> &b) {
        FastAGMSSketch mergedA = graph.tableSketches.at(a.first).merge(graph.tableSketches.at(a.second));
        FastAGMSSketch mergedB = graph.tableSketches.at(b.first).merge(graph.tableSketches.at(b.second));
        return mergedA.dotProduct(mergedA) > mergedB.dotProduct(mergedB);
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

        resultNames[tableA + "_" + tableB] = newResult;
        resultNames[tableA] = newResult;
        resultNames[tableB] = newResult;

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

    // Define the example query similar to JOB benchmark format
    std::string query = R"(
        SELECT MIN(table_a.value) AS min_value,
               MIN(table_b.name) AS name,
               MIN(table_c.date) AS date
        FROM table_a,
             table_b,
             table_c
        WHERE table_a.value > 100
          AND table_b.type = 'example'
          AND table_a.id = table_b.id
          AND table_b.id = table_c.id
          AND table_a.id = table_c.id;
    )";

    // Analyze the query
    QueryInfo queryInfo = analyzeQuery(query);
    
    // Create the join graph
    JoinGraph graph;

    // Build sketches for each table with sample queries
    for (const auto& table : queryInfo.tables) {
        // Clean the table name by removing any trailing commas
        std::string cleanTable = table;
        cleanTable.erase(std::remove(cleanTable.begin(), cleanTable.end(), ','), cleanTable.end());
        
        std::string sampleQuery = "SELECT id FROM " + cleanTable + " LIMIT 1000";
        try {
            graph.tableSketches[cleanTable] = buildSketchFromQuery(conn, sampleQuery, 10, 50);
            std::cout << "Built sketch for " << cleanTable << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "Error building sketch for " << cleanTable << ": " << e.what() << std::endl;
            // Continue with other tables instead of failing completely
            continue;
        }
    }

    // Add joins based on the query analysis
    for (size_t i = 0; i < queryInfo.tables.size(); ++i) {
        std::string table1 = queryInfo.tables[i];
        table1.erase(std::remove(table1.begin(), table1.end(), ','), table1.end());
        
        for (size_t j = i + 1; j < queryInfo.tables.size(); ++j) {
            std::string table2 = queryInfo.tables[j];
            table2.erase(std::remove(table2.begin(), table2.end(), ','), table2.end());
            
            // Only add joins if both tables have sketches
            if (graph.tableSketches.count(table1) && graph.tableSketches.count(table2)) {
                graph.joins.push_back({table1, table2});
                // Add join conditions
                std::string key = table1 + "_" + table2;
                graph.joinConditions[key] = {table1 + ".id = " + table2 + ".id"};
            }
        }
    }

    // Print extracted information
    std::cout << "\nExtracted Tables:" << std::endl;
    for (const auto& table : queryInfo.tables) {
        std::cout << "- " << table << std::endl;
    }

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
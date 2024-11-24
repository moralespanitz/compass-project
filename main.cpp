#include <iostream>
#include <libpq-fe.h>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <queue>
#include <sstream>
#include <random>
#include <fstream>
#include <sstream>

// Clase FastAGMSSketch
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

// Función para obtener los valores de una tabla y construir un sketch
FastAGMSSketch buildSketchFromTable(PGconn *conn, const std::string &tableName, int rows, int cols) {
    FastAGMSSketch sketch(rows, cols);

    std::string query = "SELECT DISTINCT value FROM " + tableName + ";";
    PGresult *res = PQexec(conn, query.c_str());

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::cerr << "Query for table " << tableName << " failed: " << PQerrorMessage(conn) << std::endl;
        PQclear(res);
        PQfinish(conn);
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

// Representar el grafo de joins
struct JoinGraph {
    std::unordered_map<std::string, FastAGMSSketch> tableSketches;
    std::vector<std::pair<std::string, std::string>> joins;  // Lista de joins (tabla_a, table_b)
};

// Función para construir el plan textual
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

        std::string newResult = "(" + left + " ⨝ " + right + ")";
        joinPlan = newResult;

        resultNames[tableA + "_" + tableB] = newResult;
        resultNames[tableA] = newResult;
        resultNames[tableB] = newResult;

        joinedTables.insert(tableA);
        joinedTables.insert(tableB);
    }

    return joinPlan;
}

// SQLProcessor class for handling SQL file processing
class SQLProcessor {
private:
    PGconn* conn;
    
    std::string trim(const std::string& str) {
        size_t first = str.find_first_not_of(" \t\n\r");
        if (first == std::string::npos) return "";
        size_t last = str.find_last_not_of(" \t\n\r");
        return str.substr(first, (last - first + 1));
    }

    bool executeQuery(const std::string& query) {
        std::string trimmedQuery = trim(query);
        if (trimmedQuery.empty()) {
            return true;
        }

        std::cout << "Executing query: " << trimmedQuery << std::endl;

        PGresult* res = PQexec(conn, trimmedQuery.c_str());
        if (!res) {
            std::cerr << "Memory allocation error or lost connection" << std::endl;
            return false;
        }

        ExecStatusType status = PQresultStatus(res);
        
        if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
            std::cerr << "Error executing query: " << PQerrorMessage(conn) << std::endl;
            std::cerr << "Status: " << PQresStatus(status) << std::endl;
            PQclear(res);
            return false;
        }

        if (status == PGRES_TUPLES_OK) {
            int nFields = PQnfields(res);
            int nRows = PQntuples(res);
            
            for (int i = 0; i < nFields; i++) {
                std::cout << PQfname(res, i) << "\t";
            }
            std::cout << std::endl;

            for (int i = 0; i < nRows; i++) {
                for (int j = 0; j < nFields; j++) {
                    std::cout << (PQgetisnull(res, i, j) ? "NULL" : PQgetvalue(res, i, j)) << "\t";
                }
                std::cout << std::endl;
            }
        }

        PQclear(res);
        return true;
    }

public:
    SQLProcessor(PGconn* existingConn) : conn(existingConn) {
        if (!conn || PQstatus(conn) != CONNECTION_OK) {
            throw std::runtime_error("Invalid database connection provided");
        }
    }

    bool processFile(const std::string& filename) {
        std::ifstream file(filename);
        if (!file.is_open()) {
            std::cerr << "Error: Could not open file " << filename << std::endl;
            return false;
        }

        std::stringstream buffer;
        buffer << file.rdbuf();
        std::string content = buffer.str();
        file.close();

        std::string query;
        bool inQuotes = false;
        bool inComment = false;
        bool success = true;
        
        for (size_t i = 0; i < content.length(); i++) {
            char c = content[i];
            char next = (i + 1 < content.length()) ? content[i + 1] : '\0';

            if (!inQuotes && c == '-' && next == '-') {
                inComment = true;
                i++;
                continue;
            }
            if (inComment && c == '\n') {
                inComment = false;
                continue;
            }
            if (inComment) {
                continue;
            }

            if (c == '\'') {
                if (inQuotes && next == '\'') {
                    query += c;
                    query += next;
                    i++;
                    continue;
                }
                inQuotes = !inQuotes;
            }

            if (c == ';' && !inQuotes) {
                if (!executeQuery(query)) {
                    success = false;
                    std::cerr << "Failed query: " << query << std::endl;
                }
                query.clear();
            } else {
                query += c;
            }
        }

        query = trim(query);
        if (!query.empty() && !executeQuery(query)) {
            success = false;
            std::cerr << "Failed final query: " << query << std::endl;
        }

        return success;
    }
};

int main(int argc, char* argv[]) {
    const char *conninfo = "dbname=mydatabase user=myuser password=mypassword hostaddr=127.0.0.1 port=5432";
    PGconn *conn = PQconnectdb(conninfo);

    if (PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "Connection to database failed: " << PQerrorMessage(conn) << std::endl;
        PQfinish(conn);
        return 1;
    }
    std::cout << "Connected to PostgreSQL!" << std::endl;

    try {
        // Process SQL file if provided as argument
        if (argc > 1) {
            SQLProcessor processor(conn);
            if (!processor.processFile(argv[1])) {
                std::cerr << "Error processing SQL file." << std::endl;
                PQfinish(conn);
                return 1;
            }
            std::cout << "SQL file processed successfully." << std::endl;
        }

        // Proceed with COMPASS join planning
        JoinGraph graph;
        graph.tableSketches["table_a"] = buildSketchFromTable(conn, "table_a", 10, 50);
        graph.tableSketches["table_b"] = buildSketchFromTable(conn, "table_b", 10, 50);
        graph.tableSketches["table_c"] = buildSketchFromTable(conn, "table_c", 10, 50);

        graph.joins = {{"table_a", "table_b"}, {"table_b", "table_c"}};

        std::string joinPlan = buildJoinPlan(graph);
        std::cout << "Plan textual generado por COMPASS:\n" << joinPlan << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        PQfinish(conn);
        return 1;
    }

    PQfinish(conn);
    return 0;
}
#include <iostream>
#include <libpq-fe.h>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <queue>
#include <sstream>
#include <random>

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

int main() {
    const char *conninfo = "dbname=mydatabase user=myuser password=mypassword hostaddr=127.0.0.1 port=5432";
    PGconn *conn = PQconnectdb(conninfo);

    if (PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "Connection to database failed: " << PQerrorMessage(conn) << std::endl;
        PQfinish(conn);
        return 1;
    }
    std::cout << "Connected to PostgreSQL!" << std::endl;

    JoinGraph graph;
    graph.tableSketches["table_a"] = buildSketchFromTable(conn, "table_a", 10, 50);
    graph.tableSketches["table_b"] = buildSketchFromTable(conn, "table_b", 10, 50);
    graph.tableSketches["table_c"] = buildSketchFromTable(conn, "table_c", 10, 50);

    graph.joins = {{"table_a", "table_b"}, {"table_b", "table_c"}};

    std::string joinPlan = buildJoinPlan(graph);
    std::cout << "Plan textual generado por COMPASS:\n" << joinPlan << std::endl;

    PQfinish(conn);
    return 0;
}
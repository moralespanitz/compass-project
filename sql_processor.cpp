#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <libpq-fe.h>
#include <algorithm>

class SQLProcessor {
private:
    PGconn* conn;
    
    // Helper function to trim whitespace
    std::string trim(const std::string& str) {
        size_t first = str.find_first_not_of(" \t\n\r");
        if (first == std::string::npos) return "";
        size_t last = str.find_last_not_of(" \t\n\r");
        return str.substr(first, (last - first + 1));
    }

    // Helper function to execute a single SQL statement
    bool executeQuery(const std::string& query) {
        std::string trimmedQuery = trim(query);
        if (trimmedQuery.empty()) {
            return true;
        }

        // Debug output
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

        // Print results if it's a SELECT query
        if (status == PGRES_TUPLES_OK) {
            int nFields = PQnfields(res);
            int nRows = PQntuples(res);
            
            // Print column headers
            for (int i = 0; i < nFields; i++) {
                std::cout << PQfname(res, i) << "\t";
            }
            std::cout << std::endl;

            // Print rows
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
    SQLProcessor(const char* conninfo) {
        conn = PQconnectdb(conninfo);
        if (PQstatus(conn) != CONNECTION_OK) {
            throw std::runtime_error("Connection to database failed: " + std::string(PQerrorMessage(conn)));
        }
    }

    ~SQLProcessor() {
        if (conn) {
            PQfinish(conn);
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

        // Split the content into individual queries
        std::string query;
        bool inQuotes = false;
        bool inComment = false;
        bool success = true;
        
        for (size_t i = 0; i < content.length(); i++) {
            char c = content[i];
            char next = (i + 1 < content.length()) ? content[i + 1] : '\0';

            // Handle comments
            if (!inQuotes && c == '-' && next == '-') {
                inComment = true;
                i++; // Skip next character
                continue;
            }
            if (inComment && c == '\n') {
                inComment = false;
                continue;
            }
            if (inComment) {
                continue;
            }

            // Handle quotes
            if (c == '\'') {
                if (inQuotes && next == '\'') {
                    // Handle escaped quote
                    query += c;
                    query += next;
                    i++; // Skip next character
                    continue;
                }
                inQuotes = !inQuotes;
            }

            // Process query termination
            if (c == ';' && !inQuotes) {
                // Execute the query and check for success
                if (!executeQuery(query)) {
                    success = false;
                    std::cerr << "Failed query: " << query << std::endl;
                }
                query.clear();
            } else {
                query += c;
            }
        }

        // Execute any remaining query
        query = trim(query);
        if (!query.empty() && !executeQuery(query)) {
            success = false;
            std::cerr << "Failed final query: " << query << std::endl;
        }

        return success;
    }
};

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <sql_file>" << std::endl;
        return 1;
    }

    try {
        const char* conninfo = "dbname=job user=postgres password=postgres hostaddr=127.0.0.1 port=5432";
        std::cout << "Attempting to connect with: " << conninfo << std::endl;
        
        SQLProcessor processor(conninfo);
        
        if (processor.processFile(argv[1])) {
            std::cout << "SQL file processed successfully." << std::endl;
            return 0;
        } else {
            std::cerr << "Error processing SQL file." << std::endl;
            return 1;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
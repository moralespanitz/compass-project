#include <iostream>
#include <libpq-fe.h>
#include <string>
#include <regex>
#include <stack>

// Función para obtener el plan JSON del join directamente desde PostgreSQL
std::string getJoinPlanJSON(PGconn *conn, const std::string &query) {
    std::string explainQuery = "EXPLAIN (FORMAT JSON) " + query;
    PGresult *res = PQexec(conn, explainQuery.c_str());

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::cerr << "Error al ejecutar la consulta EXPLAIN: " << PQerrorMessage(conn) << std::endl;
        PQclear(res);
        throw std::runtime_error("No se pudo obtener el plan del join.");
    }

    // Combinar todas las líneas del resultado de EXPLAIN en una sola cadena JSON
    std::string jsonPlan = PQgetvalue(res, 0, 0);
    PQclear(res);
    return jsonPlan;
}

// Función para analizar manualmente el JSON y construir el plan de joins
std::string parseJSONPlan(const std::string &jsonPlan) {
    // Expresiones regulares para detectar los nombres de las tablas y los operadores de join
    std::regex seqScanRegex(R"("Relation Name":\s*\"(\w+)\")");
    std::regex joinRegex(R"("Node Type":\s*\"(Merge Join|Nested Loop|Hash Join)\")");
    std::smatch match;

    std::stack<std::string> joinStack;
    auto begin = jsonPlan.cbegin();
    auto end = jsonPlan.cend();

    // Extraer todas las tablas (Seq Scan) y añadirlas a la pila
    while (std::regex_search(begin, end, match, seqScanRegex)) {
        joinStack.push(match[1]); // Agregar tabla a la pila
        begin = match.suffix().first;
    }

    // Reiniciar el análisis para procesar los operadores de join
    begin = jsonPlan.cbegin();
    while (std::regex_search(begin, end, match, joinRegex)) {
        if (joinStack.size() >= 2) {
            std::string right = joinStack.top();
            joinStack.pop();
            std::string left = joinStack.top();
            joinStack.pop();
            std::string combined = "(" + left + " ⨝ " + right + ")";
            joinStack.push(combined); // Agregar el resultado combinado
        }
        begin = match.suffix().first;
    }

    return joinStack.empty() ? "Error: No se pudo construir el plan." : joinStack.top();
}

int main() {
    const char *conninfo = "dbname=job user=postgres password=postgres hostaddr=127.0.0.1 port=5432";
    PGconn *conn = PQconnectdb(conninfo);

    if (PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "Conexión fallida: " << PQerrorMessage(conn) << std::endl;
        PQfinish(conn);
        return 1;
    }
    std::cout << "Conexión a PostgreSQL exitosa." << std::endl;

    // Consulta SQL compleja
    std::string query = R"SQL(
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
    )SQL";


    try {
        // Obtener el plan JSON
        std::string jsonPlan = getJoinPlanJSON(conn, query);

        // Analizar el JSON para construir el plan de joins
        std::string formattedPlan = parseJSONPlan(jsonPlan);
        // std::cout << "Plan de joins en formato ((table_a ⨝ table_b) ⨝ table_c):\n" << formattedPlan << std::endl;
        std::cout << "Join Plan:\n" << formattedPlan << std::endl;
    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    PQfinish(conn);
    return 0;
}

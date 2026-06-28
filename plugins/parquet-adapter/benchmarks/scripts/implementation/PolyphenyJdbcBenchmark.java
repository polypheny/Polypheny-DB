import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PolyphenyJdbcBenchmark {

    private static final String DEFAULT_URL = "jdbc:polypheny://pa:@localhost:20590/public";

    private record Query(String id, String description, String sql) {
    }

    private record Config(
            String url,
            Path queries,
            Path output,
            Path resultValuesOutput,
            int warmups,
            int runs,
            boolean polyphenyTableNames,
            String polyphenyTablePrefix,
            boolean printRows,
            int fetchSize,
            int queryTimeoutSeconds,
            String sql,
            Path sqlFile,
            Set<String> only ) {
    }


    public static void main(String[] args) throws Exception {
        Config config = parseArgs(args);
        String adHocSql = adHocSql(config);
        List<Query> queries = adHocSql.isBlank()
                ? loadQueries(config.queries(), config.polyphenyTableNames(), config.polyphenyTablePrefix())
                : List.of(new Query("SQL", "Ad hoc SQL", prepareSql(adHocSql, config.polyphenyTableNames(), config.polyphenyTablePrefix())));
        if (!config.only().isEmpty()) {
            queries = queries.stream()
                    .filter(query -> config.only().contains(query.id().toUpperCase(Locale.ROOT)))
                    .toList();
        }
        if (queries.isEmpty()) {
            throw new IllegalArgumentException("No queries found in " + config.queries());
        }

        Class.forName("org.polypheny.jdbc.PolyphenyDriver");

        if (config.output().getParent() != null) {
            Files.createDirectories(config.output().getParent());
        }

        try (Connection connection = DriverManager.getConnection(config.url());
                BufferedWriter writer = Files.newBufferedWriter(config.output(), StandardCharsets.UTF_8)) {
            connection.setReadOnly(true);
            writer.write("timestamp,query_id,description,phase,run,elapsed_ms,rows,columns,success,error");
            writer.newLine();
            writer.flush();

            System.out.printf("Connected to %s%n", config.url());
            if (adHocSql.isBlank()) {
                System.out.printf("Loaded %d queries from %s%n", queries.size(), config.queries());
            } else {
                System.out.printf("Loaded ad hoc SQL query%n");
            }
            System.out.printf("Writing CSV to %s%n", config.output());

            for (Query query : queries) {
                System.out.printf("%n%s %s%n", query.id(), query.description());
                for (int i = 1; i <= config.warmups(); i++) {
                    executeAndRecord(connection, writer, query, "warmup", i, config);
                }
                for (int i = 1; i <= config.runs(); i++) {
                    executeAndRecord(connection, writer, query, "measured", i, config);
                }
            }

            captureResultValues(connection, queries, config);
        }
    }


    private static String adHocSql(Config config) throws IOException {
        if (config.sqlFile() != null) {
            return Files.readString(config.sqlFile(), StandardCharsets.UTF_8);
        }
        return config.sql();
    }


    private static void executeAndRecord(Connection connection, BufferedWriter writer, Query query, String phase, int run, Config config) throws IOException {
        long start = System.nanoTime();
        long rows = 0;
        int columns = 0;
        boolean success = false;
        String error = "";

        try (Statement statement = connection.createStatement()) {
            if (config.fetchSize() > 0) {
                statement.setFetchSize(config.fetchSize());
            }
            if (config.queryTimeoutSeconds() > 0) {
                statement.setQueryTimeout(config.queryTimeoutSeconds());
            }

            boolean hasResultSet = statement.execute(query.sql());
            if (hasResultSet) {
                try (ResultSet resultSet = statement.getResultSet()) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    columns = metaData.getColumnCount();
                    while (resultSet.next()) {
                        rows++;
                        if (config.printRows()) {
                            printRow(resultSet, metaData);
                        }
                    }
                }
            } else {
                rows = statement.getUpdateCount();
            }
            success = true;
        } catch (SQLException e) {
            error = e.getClass().getSimpleName() + ": " + e.getMessage();
            System.out.printf("  %s %d failed: %s%n", phase, run, error);
        }

        long elapsedMs = Math.round((System.nanoTime() - start) / 1_000_000.0);
        if (success) {
            System.out.printf("  %s %d: %d ms, rows=%d%n", phase, run, elapsedMs, rows);
        }
        writeCsvRow(writer, query, phase, run, elapsedMs, rows, columns, success, error);
    }


    private static void captureResultValues(Connection connection, List<Query> queries, Config config) throws IOException {
        if (config.resultValuesOutput() == null) {
            return;
        }
        if (config.resultValuesOutput().getParent() != null) {
            Files.createDirectories(config.resultValuesOutput().getParent());
        }
        try (BufferedWriter writer = Files.newBufferedWriter(config.resultValuesOutput(), StandardCharsets.UTF_8)) {
            System.out.printf("Writing result values to %s%n", config.resultValuesOutput());
            for (Query query : queries) {
                writeResultValues(connection, writer, query, config);
            }
        }
    }


    private static void writeResultValues(Connection connection, BufferedWriter writer, Query query, Config config) throws IOException {
        List<String> columns = new ArrayList<>();
        List<List<String>> rows = new ArrayList<>();
        boolean success = false;
        String error = "";

        try (Statement statement = connection.createStatement()) {
            if (config.fetchSize() > 0) {
                statement.setFetchSize(config.fetchSize());
            }
            if (config.queryTimeoutSeconds() > 0) {
                statement.setQueryTimeout(config.queryTimeoutSeconds());
            }

            boolean hasResultSet = statement.execute(query.sql());
            if (hasResultSet) {
                try (ResultSet resultSet = statement.getResultSet()) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        columns.add(metaData.getColumnLabel(i));
                    }
                    while (resultSet.next()) {
                        List<String> row = new ArrayList<>();
                        for (int i = 1; i <= metaData.getColumnCount(); i++) {
                            row.add(valueToString(resultSet.getObject(i)));
                        }
                        rows.add(row);
                    }
                }
            } else {
                columns.add("update_count");
                rows.add(List.of(Long.toString(statement.getUpdateCount())));
            }
            success = true;
        } catch (SQLException e) {
            error = e.getClass().getSimpleName() + ": " + e.getMessage();
        }

        writer.write(resultValuesJson(query, success, error, columns, rows));
        writer.newLine();
        writer.flush();
    }


    private static void printRow(ResultSet resultSet, ResultSetMetaData metaData) throws SQLException {
        List<String> values = new ArrayList<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            Object value = resultSet.getObject(i);
            values.add(value == null ? "NULL" : value.toString());
        }
        System.out.println(String.join("\t", values));
    }


    private static void writeCsvRow(BufferedWriter writer, Query query, String phase, int run, long elapsedMs, long rows, int columns, boolean success, String error) throws IOException {
        writer.write(String.join(",",
                csv(Instant.now().toString()),
                csv(query.id()),
                csv(query.description()),
                csv(phase),
                Integer.toString(run),
                Long.toString(elapsedMs),
                Long.toString(rows),
                Integer.toString(columns),
                Boolean.toString(success),
                csv(error)));
        writer.newLine();
        writer.flush();
    }


    private static String csv(String value) {
        String escaped = value.replace("\"", "\"\"");
        return "\"" + escaped + "\"";
    }


    private static String resultValuesJson(Query query, boolean success, String error, List<String> columns, List<List<String>> rows) {
        StringBuilder json = new StringBuilder();
        json.append('{');
        appendJsonField(json, "timestamp", Instant.now().toString());
        json.append(',');
        appendJsonField(json, "query_id", query.id());
        json.append(',');
        appendJsonField(json, "description", query.description());
        json.append(",\"success\":").append(success);
        json.append(',');
        appendJsonField(json, "error", error);
        json.append(",\"columns\":");
        appendJsonStringArray(json, columns);
        json.append(",\"rows\":");
        appendJsonRows(json, rows);
        json.append('}');
        return json.toString();
    }


    private static void appendJsonField(StringBuilder json, String name, String value) {
        json.append('"').append(jsonEscape(name)).append("\":").append(jsonString(value));
    }


    private static void appendJsonStringArray(StringBuilder json, List<String> values) {
        json.append('[');
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                json.append(',');
            }
            json.append(jsonString(values.get(i)));
        }
        json.append(']');
    }


    private static void appendJsonRows(StringBuilder json, List<List<String>> rows) {
        json.append('[');
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            if (rowIndex > 0) {
                json.append(',');
            }
            appendJsonStringArray(json, rows.get(rowIndex));
        }
        json.append(']');
    }


    private static String jsonString(String value) {
        return value == null ? "null" : "\"" + jsonEscape(value) + "\"";
    }


    private static String jsonEscape(String value) {
        StringBuilder escaped = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\b' -> escaped.append("\\b");
                case '\f' -> escaped.append("\\f");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (ch < 0x20) {
                        escaped.append(String.format("\\u%04x", (int) ch));
                    } else {
                        escaped.append(ch);
                    }
                }
            }
        }
        return escaped.toString();
    }


    private static String valueToString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.toPlainString();
        }
        return value.toString();
    }


    private static List<Query> loadQueries(Path path, boolean polyphenyTableNames, String polyphenyTablePrefix) throws IOException {
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        List<Query> queries = new ArrayList<>();
        String currentId = null;
        String currentDescription = null;
        StringBuilder sql = new StringBuilder();

        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.startsWith("-- Q") && trimmed.contains(":")) {
                addQuery(queries, currentId, currentDescription, sql, polyphenyTableNames, polyphenyTablePrefix);
                int colon = trimmed.indexOf(':');
                currentId = trimmed.substring(3, colon).trim();
                currentDescription = trimmed.substring(colon + 1).trim();
                sql.setLength(0);
                continue;
            }
            if (currentId == null || trimmed.startsWith("--")) {
                continue;
            }
            sql.append(line).append('\n');
            if (trimmed.endsWith(";")) {
                addQuery(queries, currentId, currentDescription, sql, polyphenyTableNames, polyphenyTablePrefix);
                currentId = null;
                currentDescription = null;
                sql.setLength(0);
            }
        }
        addQuery(queries, currentId, currentDescription, sql, polyphenyTableNames, polyphenyTablePrefix);
        return queries;
    }


    private static void addQuery(List<Query> queries, String id, String description, StringBuilder sql, boolean polyphenyTableNames, String polyphenyTablePrefix) {
        if (id == null) {
            return;
        }
        String statement = sql.toString().trim();
        if (statement.isEmpty()) {
            return;
        }
        queries.add(new Query(id, description == null ? "" : description, prepareSql(statement, polyphenyTableNames, polyphenyTablePrefix)));
    }


    private static String prepareSql(String sql, boolean polyphenyTableNames, String polyphenyTablePrefix) {
        String statement = sql.trim();
        if (statement.endsWith(";")) {
            statement = statement.substring(0, statement.length() - 1);
        }
        if (polyphenyTableNames) {
            statement = mapToPolyphenyTableNames(statement, polyphenyTablePrefix);
        }
        return statement;
    }


    private static String mapToPolyphenyTableNames(String sql, String tablePrefix) {
        return sql
                .replaceAll("(?i)\\byellow_tripdata\\b", tablePrefix + "yellow_tripdata")
                .replaceAll("(?i)\\bgreen_tripdata\\b", tablePrefix + "green_tripdata")
                .replaceAll("(?i)\\bfhv_tripdata\\b", tablePrefix + "fhv_tripdata")
                .replaceAll("(?i)\\bfhvhv_tripdata\\b", tablePrefix + "fhvhv_tripdata");
    }


    private static Config parseArgs(String[] args) {
        Map<String, String> values = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("Unexpected argument: " + arg);
            }
            String key;
            String value;
            int equals = arg.indexOf('=');
            if (equals >= 0) {
                key = arg.substring(2, equals);
                value = arg.substring(equals + 1);
            } else {
                key = arg.substring(2);
                if (isBooleanFlag(key)) {
                    value = "true";
                } else {
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("Missing value for " + arg);
                    }
                    value = args[++i];
                }
            }
            values.put(key.toLowerCase(Locale.ROOT), value);
        }

        Path defaultQueries = Path.of("plugins", "parquet-adapter", "benchmarks", "query_lists", "access_model_comparison", "access_model_comparison_rf.sql");
        Path defaultOutput = Path.of("plugins", "parquet-adapter", "benchmarks", "results", "access_model_comparison", "polypheny_results.csv");
        Path sqlFile = values.containsKey("sql-file") ? Path.of(values.get("sql-file")) : null;

        return new Config(
                values.getOrDefault("url", DEFAULT_URL),
                Path.of(values.getOrDefault("queries", defaultQueries.toString())),
                Path.of(values.getOrDefault("output", defaultOutput.toString())),
                optionalPath(values.get("result-values-output")),
                parseInt(values, "warmups", 1),
                parseInt(values, "runs", 5),
                parseBoolean(values, "polypheny-table-names", true),
                parseTablePrefix(values),
                parseBoolean(values, "print-rows", false),
                parseInt(values, "fetch-size", 1000),
                parseInt(values, "query-timeout", 0),
                values.getOrDefault("sql", ""),
                sqlFile,
                parseOnly(values.get("only")));
    }


    private static String parseTablePrefix(Map<String, String> values) {
        String prefix = values.getOrDefault("polypheny-table-prefix", "tlc__");
        if (!prefix.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("Invalid Polypheny table prefix: " + prefix);
        }
        return prefix;
    }


    private static Path optionalPath(String value) {
        return value == null || value.isBlank() ? null : Path.of(value);
    }


    private static boolean isBooleanFlag(String key) {
        return key.equalsIgnoreCase("polypheny-table-names")
                || key.equalsIgnoreCase("print-rows");
    }


    private static int parseInt(Map<String, String> values, String key, int defaultValue) {
        String value = values.get(key);
        return value == null ? defaultValue : Integer.parseInt(value);
    }


    private static boolean parseBoolean(Map<String, String> values, String key, boolean defaultValue) {
        String value = values.get(key);
        return value == null ? defaultValue : Boolean.parseBoolean(value);
    }


    private static Set<String> parseOnly(String value) {
        if (value == null || value.isBlank()) {
            return Set.of();
        }
        return List.of(value.split(",")).stream()
                .map(String::trim)
                .filter(part -> !part.isEmpty())
                .map(part -> part.toUpperCase(Locale.ROOT))
                .collect(Collectors.toSet());
    }
}

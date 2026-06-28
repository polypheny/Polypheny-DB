import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.polypheny.jdbc.dependency.prism.CloseStatementRequest;
import org.polypheny.jdbc.dependency.prism.ColumnMeta;
import org.polypheny.jdbc.dependency.prism.ConnectionProperties;
import org.polypheny.jdbc.dependency.prism.ConnectionRequest;
import org.polypheny.jdbc.dependency.prism.DisconnectRequest;
import org.polypheny.jdbc.dependency.prism.DocumentFrame;
import org.polypheny.jdbc.dependency.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.jdbc.dependency.prism.FetchRequest;
import org.polypheny.jdbc.dependency.prism.Frame;
import org.polypheny.jdbc.dependency.prism.ProtoDocument;
import org.polypheny.jdbc.dependency.prism.ProtoEntry;
import org.polypheny.jdbc.dependency.prism.ProtoValue;
import org.polypheny.jdbc.dependency.prism.RelationalFrame;
import org.polypheny.jdbc.dependency.prism.Request;
import org.polypheny.jdbc.dependency.prism.Response;
import org.polypheny.jdbc.dependency.prism.Row;
import org.polypheny.jdbc.dependency.prism.StatementResponse;
import org.polypheny.jdbc.dependency.prism.StatementResult;

public class PolyphenyMqlBenchmark {

    private static final String PLAIN_TRANSPORT_VERSION = "plain-v1@polypheny.com";

    private record Query(String id, String description, String mql) {
    }

    private record Config(
            String host,
            int port,
            String username,
            String password,
            String namespace,
            String language,
            int apiMajor,
            int apiMinor,
            Path queries,
            Path output,
            Path resultValuesOutput,
            int warmups,
            int runs,
            int fetchSize,
            boolean printRows,
            String mql,
            Path mqlFile,
            Set<String> only ) {
    }

    private record RunResult(long rows, int columns) {
    }

    private record ResultValues(List<String> columns, List<List<String>> rows) {
    }

    private record FrameStats(long rows, int columns) {
    }


    public static void main(String[] args) throws Exception {
        Config config = parseArgs(args);
        String adHocMql = adHocMql(config);
        List<Query> queries = adHocMql.isBlank()
                ? loadQueries(config.queries())
                : List.of(new Query("MQL", "Ad hoc MQL", prepareMql(adHocMql)));
        if (!config.only().isEmpty()) {
            queries = queries.stream()
                    .filter(query -> config.only().contains(query.id().toUpperCase(Locale.ROOT)))
                    .toList();
        }
        if (queries.isEmpty()) {
            throw new IllegalArgumentException("No queries found in " + config.queries());
        }

        if (config.output().getParent() != null) {
            Files.createDirectories(config.output().getParent());
        }

        try (PrismClient client = PrismClient.connect(config);
                BufferedWriter writer = Files.newBufferedWriter(config.output(), StandardCharsets.UTF_8)) {
            writer.write("timestamp,query_id,description,phase,run,elapsed_ms,rows,columns,success,error");
            writer.newLine();
            writer.flush();

            System.out.printf("Connected to prism://%s:%d as %s%n", config.host(), config.port(), config.username());
            System.out.printf("Using language=%s namespace=%s fetchSize=%d%n", config.language(), config.namespace(), config.fetchSize());
            if (adHocMql.isBlank()) {
                System.out.printf("Loaded %d queries from %s%n", queries.size(), config.queries());
            } else {
                System.out.printf("Loaded ad hoc MQL query%n");
            }
            System.out.printf("Writing CSV to %s%n", config.output());

            for (Query query : queries) {
                System.out.printf("%n%s %s%n", query.id(), query.description());
                for (int i = 1; i <= config.warmups(); i++) {
                    executeAndRecord(client, writer, query, "warmup", i, config);
                }
                for (int i = 1; i <= config.runs(); i++) {
                    executeAndRecord(client, writer, query, "measured", i, config);
                }
            }

            captureResultValues(client, queries, config);
        }
    }


    private static String adHocMql(Config config) throws IOException {
        if (config.mqlFile() != null) {
            return Files.readString(config.mqlFile(), StandardCharsets.UTF_8);
        }
        return config.mql();
    }


    private static void executeAndRecord(PrismClient client, BufferedWriter writer, Query query, String phase, int run, Config config) throws IOException {
        long start = System.nanoTime();
        long rows = 0;
        int columns = 0;
        boolean success = false;
        String error = "";

        try {
            RunResult result = client.executeAndDrain(query.mql(), config);
            rows = result.rows();
            columns = result.columns();
            success = true;
        } catch (Exception e) {
            error = e.getClass().getSimpleName() + ": " + e.getMessage();
            System.out.printf("  %s %d failed: %s%n", phase, run, error);
        }

        long elapsedMs = Math.round((System.nanoTime() - start) / 1_000_000.0);
        if (success) {
            System.out.printf("  %s %d: %d ms, rows=%d%n", phase, run, elapsedMs, rows);
        }
        writeCsvRow(writer, query, phase, run, elapsedMs, rows, columns, success, error);
    }


    private static void captureResultValues(PrismClient client, List<Query> queries, Config config) throws IOException {
        if (config.resultValuesOutput() == null) {
            return;
        }
        if (config.resultValuesOutput().getParent() != null) {
            Files.createDirectories(config.resultValuesOutput().getParent());
        }
        try (BufferedWriter writer = Files.newBufferedWriter(config.resultValuesOutput(), StandardCharsets.UTF_8)) {
            System.out.printf("Writing result values to %s%n", config.resultValuesOutput());
            for (Query query : queries) {
                writeResultValues(client, writer, query, config);
            }
        }
    }


    private static void writeResultValues(PrismClient client, BufferedWriter writer, Query query, Config config) throws IOException {
        List<String> columns = List.of();
        List<List<String>> rows = List.of();
        boolean success = false;
        String error = "";

        try {
            ResultValues values = client.executeAndCollectValues(query.mql(), config);
            columns = values.columns();
            rows = values.rows();
            success = true;
        } catch (Exception e) {
            error = e.getClass().getSimpleName() + ": " + e.getMessage();
        }

        writer.write(resultValuesJson(query, success, error, columns, rows));
        writer.newLine();
        writer.flush();
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


    private static List<Query> loadQueries(Path path) throws IOException {
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        List<Query> queries = new ArrayList<>();
        String currentId = null;
        String currentDescription = null;
        StringBuilder mql = new StringBuilder();

        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.startsWith("-- Q") && trimmed.contains(":")) {
                addQuery(queries, currentId, currentDescription, mql);
                int colon = trimmed.indexOf(':');
                currentId = trimmed.substring(3, colon).trim();
                currentDescription = trimmed.substring(colon + 1).trim();
                mql.setLength(0);
                continue;
            }
            if (currentId == null || trimmed.startsWith("--")) {
                continue;
            }
            mql.append(line).append('\n');
            if (trimmed.endsWith(";")) {
                addQuery(queries, currentId, currentDescription, mql);
                currentId = null;
                currentDescription = null;
                mql.setLength(0);
            }
        }
        addQuery(queries, currentId, currentDescription, mql);
        return queries;
    }


    private static void addQuery(List<Query> queries, String id, String description, StringBuilder mql) {
        if (id == null) {
            return;
        }
        String statement = mql.toString().trim();
        if (statement.isEmpty()) {
            return;
        }
        queries.add(new Query(id, description == null ? "" : description, prepareMql(statement)));
    }


    private static String prepareMql(String mql) {
        String statement = mql.trim();
        if (statement.endsWith(";")) {
            statement = statement.substring(0, statement.length() - 1);
        }
        return statement;
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

        Path defaultQueries = Path.of(
                "plugins",
                "parquet-adapter",
                "benchmarks",
                "query_lists",
                "access_model_comparison",
                "access_model_comparison_mql.mql");
        Path defaultOutput = Path.of("plugins", "parquet-adapter", "benchmarks", "results", "access_model_comparison", "polypheny_mql_results.csv");
        Path mqlFile = values.containsKey("mql-file") ? Path.of(values.get("mql-file")) : null;

        return new Config(
                values.getOrDefault("host", "localhost"),
                parseInt(values, "port", 20590),
                values.getOrDefault("username", "pa"),
                values.getOrDefault("password", ""),
                values.getOrDefault("namespace", "pd_document"),
                values.getOrDefault("language", "mongo"),
                parseInt(values, "api-major", 1),
                parseInt(values, "api-minor", 9),
                Path.of(values.getOrDefault("queries", defaultQueries.toString())),
                Path.of(values.getOrDefault("output", defaultOutput.toString())),
                optionalPath(values.get("result-values-output")),
                parseInt(values, "warmups", 1),
                parseInt(values, "runs", 5),
                parseInt(values, "fetch-size", 1000),
                parseBoolean(values, "print-rows", false),
                values.getOrDefault("mql", ""),
                mqlFile,
                parseOnly(values.get("only")));
    }


    private static boolean isBooleanFlag(String key) {
        return key.equalsIgnoreCase("print-rows");
    }


    private static Path optionalPath(String value) {
        return value == null || value.isBlank() ? null : Path.of(value);
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


    private static Map<String, String> documentToMap(ProtoDocument document) {
        Map<String, String> row = new LinkedHashMap<>();
        for (ProtoEntry entry : document.getEntriesList()) {
            String key = entry.hasKey() ? protoValueToString(entry.getKey()) : "";
            row.put(key, entry.hasValue() ? protoValueToString(entry.getValue()) : null);
        }
        return row;
    }


    private static String protoValueToString(ProtoValue value) {
        return switch (value.getValueCase()) {
            case BOOLEAN -> Boolean.toString(value.getBoolean().getBoolean());
            case INTEGER -> Integer.toString(value.getInteger().getInteger());
            case LONG -> Long.toString(value.getLong().getLong());
            case BIG_DECIMAL -> protoBigDecimalToString(value);
            case FLOAT -> Float.toString(value.getFloat().getFloat());
            case DOUBLE -> Double.toString(value.getDouble().getDouble());
            case DATE -> Long.toString(value.getDate().getDate());
            case TIME -> Integer.toString(value.getTime().getTime());
            case TIMESTAMP -> Long.toString(value.getTimestamp().getTimestamp());
            case INTERVAL -> value.getInterval().getMonths() + ":" + value.getInterval().getMilliseconds();
            case STRING -> value.getString().getString();
            case BINARY -> value.getBinary().getBinary().toString();
            case NULL, VALUE_NOT_SET -> null;
            case LIST -> protoListToString(value);
            case DOCUMENT -> protoDocumentToString(value.getDocument());
            case FILE -> value.getFile().getBinary().toString();
        };
    }


    private static String protoBigDecimalToString(ProtoValue value) {
        return new BigDecimal(
                new BigInteger(value.getBigDecimal().getUnscaledValue().toByteArray()),
                value.getBigDecimal().getScale()).toPlainString();
    }


    private static String protoListToString(ProtoValue value) {
        List<String> values = value.getList().getValuesList().stream()
                .map(PolyphenyMqlBenchmark::protoValueToString)
                .map(item -> item == null ? "null" : item)
                .toList();
        return "[" + String.join(",", values) + "]";
    }


    private static String protoDocumentToString(ProtoDocument document) {
        List<String> values = new ArrayList<>();
        for (ProtoEntry entry : document.getEntriesList()) {
            String key = entry.hasKey() ? protoValueToString(entry.getKey()) : "";
            String value = entry.hasValue() ? protoValueToString(entry.getValue()) : null;
            values.add(key + ":" + (value == null ? "null" : value));
        }
        return "{" + String.join(",", values) + "}";
    }


    private static final class PrismClient implements Closeable {

        private final Socket socket;
        private final DataInputStream input;
        private final OutputStream output;
        private long nextRequestId = 1;


        private PrismClient(Socket socket) throws IOException {
            this.socket = socket;
            this.input = new DataInputStream(socket.getInputStream());
            this.output = socket.getOutputStream();
        }


        static PrismClient connect(Config config) throws IOException {
            Socket socket = new Socket();
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(config.host(), config.port()));
            PrismClient client = new PrismClient(socket);
            client.exchangeVersion();
            client.authenticate(config);
            return client;
        }


        RunResult executeAndDrain(String mql, Config config) throws IOException {
            int statementId = -1;
            try {
                long requestId = nextRequestId++;
                ExecuteUnparameterizedStatementRequest executeRequest = ExecuteUnparameterizedStatementRequest.newBuilder()
                        .setLanguageName(config.language())
                        .setStatement(mql)
                        .setFetchSize(config.fetchSize())
                        .setNamespaceName(config.namespace())
                        .build();
                send(Request.newBuilder()
                        .setId(requestId)
                        .setExecuteUnparameterizedStatementRequest(executeRequest)
                        .build());

                StatementResult result = null;
                while (true) {
                    Response response = receive(requestId);
                    if (!response.hasStatementResponse()) {
                        throw new IOException("Expected statement_response, got " + response.getTypeCase());
                    }
                    StatementResponse statementResponse = response.getStatementResponse();
                    statementId = statementResponse.getStatementId();
                    if (statementResponse.hasResult()) {
                        result = statementResponse.getResult();
                    }
                    if (response.getLast()) {
                        break;
                    }
                }

                if (result == null) {
                    return new RunResult(0, 0);
                }
                if (!result.hasFrame()) {
                    return new RunResult(result.getScalar(), 0);
                }
                return drainFrames(statementId, result.getFrame(), config);
            } finally {
                if (statementId > 0) {
                    closeStatement(statementId);
                }
            }
        }


        ResultValues executeAndCollectValues(String mql, Config config) throws IOException {
            int statementId = -1;
            try {
                long requestId = nextRequestId++;
                ExecuteUnparameterizedStatementRequest executeRequest = ExecuteUnparameterizedStatementRequest.newBuilder()
                        .setLanguageName(config.language())
                        .setStatement(mql)
                        .setFetchSize(config.fetchSize())
                        .setNamespaceName(config.namespace())
                        .build();
                send(Request.newBuilder()
                        .setId(requestId)
                        .setExecuteUnparameterizedStatementRequest(executeRequest)
                        .build());

                StatementResult result = null;
                while (true) {
                    Response response = receive(requestId);
                    if (!response.hasStatementResponse()) {
                        throw new IOException("Expected statement_response, got " + response.getTypeCase());
                    }
                    StatementResponse statementResponse = response.getStatementResponse();
                    statementId = statementResponse.getStatementId();
                    if (statementResponse.hasResult()) {
                        result = statementResponse.getResult();
                    }
                    if (response.getLast()) {
                        break;
                    }
                }

                if (result == null) {
                    return new ResultValues(List.of(), List.of());
                }
                if (!result.hasFrame()) {
                    return new ResultValues(List.of("scalar"), List.of(List.of(Long.toString(result.getScalar()))));
                }
                return collectFrames(statementId, result.getFrame(), config);
            } finally {
                if (statementId > 0) {
                    closeStatement(statementId);
                }
            }
        }


        private RunResult drainFrames(int statementId, Frame firstFrame, Config config) throws IOException {
            long rows = 0;
            int columns = 0;
            Frame frame = firstFrame;
            while (true) {
                FrameStats stats = countFrame(frame, config.printRows());
                rows += stats.rows();
                columns = Math.max(columns, stats.columns());
                if (frame.getIsLast()) {
                    return new RunResult(rows, columns);
                }
                frame = fetchFrame(statementId, config.fetchSize());
            }
        }


        private ResultValues collectFrames(int statementId, Frame firstFrame, Config config) throws IOException {
            List<String> columns = new ArrayList<>();
            List<List<String>> rows = new ArrayList<>();
            List<Map<String, String>> documentRows = new ArrayList<>();
            boolean documentMode = false;
            Frame frame = firstFrame;
            while (true) {
                if (frame.hasDocumentFrame()) {
                    documentMode = true;
                    DocumentFrame documentFrame = frame.getDocumentFrame();
                    for (ProtoDocument document : documentFrame.getDocumentsList()) {
                        Map<String, String> row = documentToMap(document);
                        for (String column : row.keySet()) {
                            if (!columns.contains(column)) {
                                columns.add(column);
                            }
                        }
                        documentRows.add(row);
                    }
                } else if (frame.hasRelationalFrame()) {
                    RelationalFrame relationalFrame = frame.getRelationalFrame();
                    if (columns.isEmpty()) {
                        for (ColumnMeta meta : relationalFrame.getColumnMetaList()) {
                            columns.add(meta.getColumnLabel().isBlank() ? meta.getColumnName() : meta.getColumnLabel());
                        }
                    }
                    for (Row row : relationalFrame.getRowsList()) {
                        List<String> values = new ArrayList<>();
                        for (ProtoValue value : row.getValuesList()) {
                            values.add(protoValueToString(value));
                        }
                        rows.add(values);
                    }
                }
                if (frame.getIsLast()) {
                    if (documentMode) {
                        for (Map<String, String> documentRow : documentRows) {
                            List<String> values = new ArrayList<>();
                            for (String column : columns) {
                                values.add(documentRow.get(column));
                            }
                            rows.add(values);
                        }
                    }
                    return new ResultValues(columns, rows);
                }
                frame = fetchFrame(statementId, config.fetchSize());
            }
        }


        private FrameStats countFrame(Frame frame, boolean printRows) {
            if (frame.hasDocumentFrame()) {
                DocumentFrame documentFrame = frame.getDocumentFrame();
                int columns = 0;
                for (ProtoDocument document : documentFrame.getDocumentsList()) {
                    columns = Math.max(columns, document.getEntriesCount());
                    if (printRows) {
                        System.out.println(document);
                    }
                }
                return new FrameStats(documentFrame.getDocumentsCount(), columns);
            }
            if (frame.hasRelationalFrame()) {
                RelationalFrame relationalFrame = frame.getRelationalFrame();
                if (printRows) {
                    relationalFrame.getRowsList().forEach(System.out::println);
                }
                return new FrameStats(relationalFrame.getRowsCount(), relationalFrame.getColumnMetaCount());
            }
            return new FrameStats(0, 0);
        }


        private Frame fetchFrame(int statementId, int fetchSize) throws IOException {
            long requestId = nextRequestId++;
            FetchRequest fetchRequest = FetchRequest.newBuilder()
                    .setStatementId(statementId)
                    .setFetchSize(fetchSize)
                    .build();
            send(Request.newBuilder()
                    .setId(requestId)
                    .setFetchRequest(fetchRequest)
                    .build());

            Response response = receive(requestId);
            if (!response.hasFrame()) {
                throw new IOException("Expected frame, got " + response.getTypeCase());
            }
            return response.getFrame();
        }


        private void closeStatement(int statementId) throws IOException {
            long requestId = nextRequestId++;
            CloseStatementRequest closeRequest = CloseStatementRequest.newBuilder()
                    .setStatementId(statementId)
                    .build();
            send(Request.newBuilder()
                    .setId(requestId)
                    .setCloseStatementRequest(closeRequest)
                    .build());
            receive(requestId);
        }


        private void authenticate(Config config) throws IOException {
            long requestId = nextRequestId++;
            ConnectionProperties properties = ConnectionProperties.newBuilder()
                    .setIsAutoCommit(true)
                    .setNamespaceName(config.namespace())
                    .build();
            ConnectionRequest connectionRequest = ConnectionRequest.newBuilder()
                    .setMajorApiVersion(config.apiMajor())
                    .setMinorApiVersion(config.apiMinor())
                    .setUsername(config.username())
                    .setPassword(config.password())
                    .setConnectionProperties(properties)
                    .build();
            send(Request.newBuilder()
                    .setId(requestId)
                    .setConnectionRequest(connectionRequest)
                    .build());

            Response response = receive(requestId);
            if (!response.hasConnectionResponse()) {
                throw new IOException("Expected connection_response, got " + response.getTypeCase());
            }
            if (!response.getConnectionResponse().getIsCompatible()) {
                throw new IOException("Incompatible Prism API. Server reports "
                        + response.getConnectionResponse().getMajorApiVersion()
                        + "."
                        + response.getConnectionResponse().getMinorApiVersion());
            }
        }


        private void exchangeVersion() throws IOException {
            byte[] version = PLAIN_TRANSPORT_VERSION.getBytes(StandardCharsets.US_ASCII);
            int length = version.length + 1;
            if (length > 255) {
                throw new IOException("Plain transport version string is too long");
            }
            output.write(length);
            output.write(version);
            output.write('\n');
            output.flush();

            byte[] response = readExact(1 + length);
            if ((response[0] & 0xff) != length) {
                throw new IOException("Invalid Prism plain transport version response length");
            }
            String responseVersion = new String(response, 1, length - 1, StandardCharsets.US_ASCII);
            if (!PLAIN_TRANSPORT_VERSION.equals(responseVersion) || response[response.length - 1] != '\n') {
                throw new IOException("Invalid Prism plain transport version response");
            }
        }


        private void send(Request request) throws IOException {
            byte[] message = request.toByteArray();
            ByteBuffer length = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
            length.putLong(message.length);
            output.write(length.array());
            output.write(message);
            output.flush();
        }


        private Response receive(long requestId) throws IOException {
            byte[] lengthBytes = readExact(8);
            long length = ByteBuffer.wrap(lengthBytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
            if (length <= 0 || length > Integer.MAX_VALUE) {
                throw new IOException("Invalid Prism message length: " + length);
            }
            Response response = Response.parseFrom(readExact((int) length));
            if (response.getId() != requestId) {
                throw new IOException("Expected response id " + requestId + ", got " + response.getId());
            }
            if (response.hasErrorResponse()) {
                throw new IOException(response.getErrorResponse().getMessage());
            }
            return response;
        }


        private byte[] readExact(int length) throws IOException {
            byte[] bytes = new byte[length];
            try {
                input.readFully(bytes);
            } catch (EOFException e) {
                throw new EOFException("Prism connection closed while reading " + length + " bytes");
            }
            return bytes;
        }


        @Override
        public void close() throws IOException {
            if (!socket.isClosed()) {
                try {
                    long requestId = nextRequestId++;
                    send(Request.newBuilder()
                            .setId(requestId)
                            .setDisconnectRequest(DisconnectRequest.newBuilder().build())
                            .build());
                    receive(requestId);
                } catch (IOException ignored) {
                    // Closing the socket below is enough if the server already ended the session.
                }
            }
            socket.close();
        }
    }
}

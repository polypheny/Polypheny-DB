import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.multimodel.PolyRow;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.multimodel.RelationalResult;
import org.polypheny.jdbc.multimodel.Result;
import org.polypheny.jdbc.types.PolyArray;
import org.polypheny.jdbc.types.PolyDocument;
import org.polypheny.jdbc.types.PolyEdge;
import org.polypheny.jdbc.types.PolyGraph;
import org.polypheny.jdbc.types.PolyGraphElement;
import org.polypheny.jdbc.types.PolyPath;
import org.polypheny.jdbc.types.TypedValue;

public class Jdbc {

    public static void main(String[] args) throws Exception {
        String host = System.getenv("M23_HOST");
        String port = System.getenv("M23_PORT");
        if (host.contains(":") && !host.startsWith("[")) {
            host = "[" + host + "]";
        }
        String url = "jdbc:polypheny:http://" + host + ":" + port + "/";
        try (var connection = DriverManager.getConnection(url, System.getenv("M23_USERNAME"), System.getenv("M23_PASSWORD"));
                var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            connection.setAutoCommit(false);
            Properties properties = new Properties();
            properties.setProperty("CypherResultFormat", "relational");
            connection.setClientInfo(properties);
            PolyConnection driver = connection.unwrap(PolyConnection.class);
            for (String line; (line = input.readLine()) != null; ) {
                JsonObject response;
                try {
                    JsonObject request = JsonParser.parseString(line).getAsJsonObject();
                    response = switch (request.get("op").getAsString()) {
                        case "commit" -> {
                            connection.commit();
                            yield new JsonObject();
                        }
                        case "rollback" -> {
                            connection.rollback();
                            yield new JsonObject();
                        }
                        case "query" -> query(driver, request);
                        default -> throw new IllegalArgumentException("Unknown adapter operation");
                    };
                } catch (Exception failure) {
                    JsonObject error = new JsonObject();
                    error.addProperty("message", failure.getMessage() == null ? failure.toString() : failure.getMessage());
                    if (failure instanceof SQLException sql && sql.getSQLState() != null) {
                        error.addProperty("code", sql.getSQLState());
                    }
                    response = new JsonObject();
                    response.add("error", error);
                }
                System.out.println(response);
            }
            connection.rollback();
        }
    }

    private static JsonObject query(PolyConnection connection, JsonObject request) throws Exception {
        String namespace = request.get("namespace").isJsonNull() ? null : request.get("namespace").getAsString();
        String language = request.get("language").getAsString();
        String query = request.get("statement").getAsString();
        JsonArray columns = new JsonArray();
        JsonArray rows = new JsonArray();
        try (PolyStatement statement = connection.createPolyStatement()) {
            Result result;
            JsonElement params = request.get("params");
            if (params.isJsonNull()) {
                result = statement.execute(namespace, language, query);
            } else {
                Map<String, TypedValue> bindings = new HashMap<>();
                for (Map.Entry<String, JsonElement> entry : params.getAsJsonObject().entrySet()) {
                    bindings.put(entry.getKey(), encode(entry.getValue()));
                }
                statement.prepareNamed(namespace, language, query);
                result = statement.executePrepared(bindings);
            }
            if (result instanceof RelationalResult relational) {
                var metadata = relational.getMetadata();
                for (int column = 0; column < metadata.getColumnCount(); column++) {
                    columns.add(metadata.getColumnMeta(column).getColumnLabel());
                }
                for (PolyRow row : relational) {
                    JsonArray cells = new JsonArray();
                    for (int cell = 0; cell < row.getColumnCount(); cell++) {
                        cells.add(decode(row.get(cell).asObject()));
                    }
                    rows.add(cells);
                }
            } else if (result.getResultType() != Result.ResultType.SCALAR) {
                throw new IllegalStateException("Unexpected result type after setting CypherResultFormat: " + result.getResultType());
            }
        }
        JsonObject response = new JsonObject();
        response.add("columns", columns);
        response.add("rows", rows);
        return response;
    }

    private static TypedValue encode(JsonElement value) throws SQLException {
        if (value.isJsonNull()) {
            return TypedValue.fromNull();
        }
        if (value.isJsonObject()) {
            PolyDocument document = new PolyDocument();
            for (Map.Entry<String, JsonElement> entry : value.getAsJsonObject().entrySet()) {
                document.put(entry.getKey(), encode(entry.getValue()));
            }
            return TypedValue.fromDocument(document);
        }
        if (value.isJsonArray()) {
            List<TypedValue> elements = new ArrayList<>();
            for (JsonElement element : value.getAsJsonArray()) {
                elements.add(encode(element));
            }
            return TypedValue.fromArray(new PolyArray("NULL", elements));
        }
        JsonPrimitive scalar = value.getAsJsonPrimitive();
        if (scalar.isString()) {
            return TypedValue.fromString(scalar.getAsString());
        }
        if (scalar.isBoolean()) {
            return TypedValue.fromBoolean(scalar.getAsBoolean());
        }
        try {
            long integral = Long.parseLong(scalar.getAsString());
            return integral >= Integer.MIN_VALUE && integral <= Integer.MAX_VALUE
                    ? TypedValue.fromInteger((int) integral)
                    : TypedValue.fromLong(integral);
        } catch (NumberFormatException ignored) {
            return TypedValue.fromDouble(scalar.getAsDouble());
        }
    }

    private static JsonElement decode(Object value) throws SQLException {
        if (value == null) {
            return JsonNull.INSTANCE;
        }
        if (value instanceof String string) {
            return new JsonPrimitive(string);
        }
        if (value instanceof Boolean bool) {
            return new JsonPrimitive(bool);
        }
        if (value instanceof Number number) {
            if (number instanceof BigDecimal
                    || (number instanceof Double || number instanceof Float) && !Double.isFinite(number.doubleValue())) {
                JsonObject special = new JsonObject();
                special.addProperty("$type", "float");
                special.addProperty("value", number.toString());
                return special;
            }
            return new JsonPrimitive(number);
        }
        if (value instanceof Array array) {
            JsonArray elements = new JsonArray();
            for (Object element : (Object[]) array.getArray()) {
                elements.add(decode(element));
            }
            return elements;
        }
        if (value instanceof PolyDocument document) {
            JsonObject object = new JsonObject();
            object.addProperty("$type", "map");
            object.add("entries", decodeMap(document));
            return object;
        }
        if (value instanceof PolyGraphElement element) {
            JsonObject graphElement = new JsonObject();
            graphElement.addProperty("id", element.getId());
            JsonArray labels = new JsonArray();
            element.getLabels().forEach(labels::add);
            graphElement.add("labels", labels);
            graphElement.add("properties", decodeMap(element.getProperties()));
            if (element instanceof PolyEdge edge) {
                graphElement.addProperty("$type", "edge");
                graphElement.addProperty("left", edge.getLeft());
                graphElement.addProperty("right", edge.getRight());
            } else {
                graphElement.addProperty("$type", "node");
            }
            return graphElement;
        }
        if (value instanceof PolyGraph graph) {
            return decodeGraph("graph", graph.getNodes(), graph.getEdges());
        }
        if (value instanceof PolyPath path) {
            return decodeGraph("path", path.getNodes(), path.getEdges());
        }
        throw new SQLException("Unsupported result value " + value.getClass());
    }

    private static JsonObject decodeMap(Map<String, TypedValue> map) throws SQLException {
        JsonObject object = new JsonObject();
        for (Map.Entry<String, TypedValue> entry : map.entrySet()) {
            object.add(entry.getKey(), decode(entry.getValue().asObject()));
        }
        return object;
    }

    private static JsonObject decodeGraph(String type, List<? extends PolyGraphElement> nodes, List<PolyEdge> edges) throws SQLException {
        JsonObject graph = new JsonObject();
        graph.addProperty("$type", type);
        JsonArray encodedNodes = new JsonArray();
        for (PolyGraphElement node : nodes) {
            encodedNodes.add(decode(node));
        }
        JsonArray encodedEdges = new JsonArray();
        for (PolyEdge edge : edges) {
            encodedEdges.add(decode(edge));
        }
        graph.add("nodes", encodedNodes);
        graph.add("edges", encodedEdges);
        return graph;
    }
}

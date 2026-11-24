using System.Text.Json;
using Polypheny.Prism;
using PolyphenyDotNetDriver;
using GraphElement = PolyphenyDotNetDriver.GraphElement;

string GetEnv(string name) => Environment.GetEnvironmentVariable(name) ?? throw new InvalidOperationException($"Set {name}");

try
{
    var dsn = $"{GetEnv("M23_HOST")}:{GetEnv("M23_PORT")},{GetEnv("M23_USERNAME")}:{GetEnv("M23_PASSWORD")}";
    using var connection = new PolyphenyConnection(dsn, new PolyphenyConfig { AutoCommit = false });
    connection.Open();
    connection.SetClientInfoProperties(new Dictionary<string, string> { ["CypherResultFormat"] = "relational" });
    for (string? line; (line = Console.ReadLine()) != null; )
    {
        object response;
        try
        {
            using var request = JsonDocument.Parse(line);
            response = ExecuteRequest(connection, request.RootElement);
        }
        catch (Exception failure)
        {
            var error = new Dictionary<string, string> { ["message"] = failure.Message };
            if (failure is PolyphenyException { SqlState: string code })
            {
                error["code"] = code;
            }
            response = new { error };
        }
        Console.WriteLine(JsonSerializer.Serialize(response));
    }
    connection.BeginTransaction().Rollback();
}
catch (Exception failure)
{
    Console.Error.WriteLine(failure.Message);
    return 1;
}
return 0;

static object ExecuteRequest(PolyphenyConnection connection, JsonElement request)
{
    switch (request.GetProperty("op").GetString())
    {
        case "commit":
            connection.BeginTransaction().Commit();
            return new { };
        case "rollback":
            connection.BeginTransaction().Rollback();
            return new { };
        case "query":
            return Query(connection, request);
        default:
            throw new ArgumentException("Unknown adapter operation");
    }
}

static object Query(PolyphenyConnection connection, JsonElement request)
{
    string statement = request.GetProperty("statement").GetString()!;
    using var command = new PolyphenyCommand().WithConnection(connection).WithCommandText(statement);
    string[] columns = [];
    var rows = new List<object?[]>();
    var parameters = request.GetProperty("params");
    switch (request.GetProperty("language").GetString())
    {
        case "cypher":
            var ns = request.GetProperty("namespace").GetString();
            using (var cypher = parameters.ValueKind == JsonValueKind.Null
                ? command.ExecuteCypher(ns)
                : command.ExecuteCypher(ns, parameters.EnumerateObject().ToDictionary(p => p.Name, p => Encode(p.Value))))
            {
                if (cypher.Kind == CypherResultKind.Graph)
                {
                    throw new InvalidOperationException("Unexpected graph frame after setting CypherResultFormat");
                }
                columns = cypher.Columns.Select(column => column.ColumnLabel).ToArray();
                do
                {
                    rows.AddRange(cypher.Rows.Select(row => row.Select(Decode).ToArray()));
                } while (cypher.FetchNext());
            }
            break;
        case "sql":
            var keyword = statement.Split((char[]?) null, StringSplitOptions.RemoveEmptyEntries).First().ToUpperInvariant();
            if (keyword is "SELECT" or "WITH" or "VALUES")
            {
                using var relational = command.ExecuteReader();
                columns = Enumerable.Range(0, relational.FieldCount).Select(relational.GetName).ToArray();
                while (relational.Read())
                {
                    rows.Add(Enumerable.Range(0, relational.FieldCount).Select(i => Decode(relational.GetValue(i))).ToArray());
                }
            }
            else
            {
                command.ExecuteNonQuery();
            }
            break;
        default:
            throw new ArgumentException("Unknown query language");
    }
    return new { columns, rows };
}

static ProtoValue Encode(JsonElement value)
{
    switch (value.ValueKind)
    {
        case JsonValueKind.Null:
            return new ProtoValue { Null = new ProtoNull() };
        case JsonValueKind.String:
            return ProtoHelper.ToProtoValue(value.GetString()!);
        case JsonValueKind.True:
        case JsonValueKind.False:
            return ProtoHelper.ToProtoValue(value.GetBoolean());
        case JsonValueKind.Number:
            if (value.TryGetInt32(out int i)) return ProtoHelper.ToProtoValue(i);
            if (value.TryGetInt64(out long l)) return ProtoHelper.ToProtoValue(l);
            return ProtoHelper.ToProtoValue(value.GetDouble());
        case JsonValueKind.Array:
            return new ProtoValue { List = new ProtoList { Values = { value.EnumerateArray().Select(Encode) } } };
        case JsonValueKind.Object:
            var document = new ProtoDocument();
            foreach (var property in value.EnumerateObject())
            {
                document.Entries.Add(property.Name, Encode(property.Value));
            }
            return new ProtoValue { Document = document };
        default:
            throw new ArgumentException($"Unsupported parameter value {value.ValueKind}");
    }
}

static object? Decode(object? value) => value switch
{
    null or DBNull => null,
    PolyphenyDecimal number => Tagged("float", ("value", number.ToString())),
    double number => Tagged("float", ("value", number.ToString("R", System.Globalization.CultureInfo.InvariantCulture))),
    float number => Tagged("float", ("value", ((double)number).ToString("R", System.Globalization.CultureInfo.InvariantCulture))),
    string or bool or int or long => value,
    IDictionary<object, object> map => Tagged("map", ("entries", map.ToDictionary(p => (string)p.Key, p => Decode(p.Value)))),
    object[] list => list.Select(Decode).ToArray(),
    GraphElement element => DecodeElement(element),
    GraphPath path => Tagged("path", ("nodes", path.Nodes.Select(DecodeElement)), ("edges", path.Edges.Select(DecodeElement))),
    GraphValue graph => Tagged("graph", ("nodes", graph.Nodes.Select(DecodeElement)), ("edges", graph.Edges.Select(DecodeElement))),
    _ => throw new ArgumentException($"Unsupported result value {value.GetType()}")
};

static Dictionary<string, object> DecodeElement(GraphElement element)
{
    var properties = element.Properties.ToDictionary(p => p.Key, p => Decode(p.Value));
    var result = Tagged(element is GraphEdge ? "edge" : "node", ("id", element.Id), ("labels", element.Labels), ("properties", properties));
    if (element is GraphEdge edge)
    {
        result["left"] = edge.Left;
        result["right"] = edge.Right;
    }
    return result;
}

static Dictionary<string, object> Tagged(string type, params (string Name, object Value)[] properties)
{
    var result = new Dictionary<string, object> { ["$type"] = type };
    foreach (var (name, value) in properties)
    {
        result[name] = value;
    }
    return result;
}

#include <PolyphenyCPPDriver.h>
#include <nlohmann/json.hpp>

#include <cmath>
#include <cstdlib>
#include <iostream>
#include <limits>

using nlohmann::json;
using Types::TypedValue;
using Value = org::polypheny::prism::ProtoValue;

static std::string environment(const char *name) {
    const char *value = std::getenv(name);
    if (value == nullptr) {
        throw std::runtime_error(std::string("Set ") + name);
    }
    return value;
}

static TypedValue encode(const json &value) {
    if (value.is_null()) {
        return TypedValue();
    }
    if (value.is_boolean()) {
        return TypedValue(value.get<bool>());
    }
    if (value.is_number_integer()) {
        if (value.is_number_unsigned() && value.get<uint64_t>() > INT64_MAX) {
            throw std::out_of_range("Parameter exceeds the signed 64-bit integer range");
        }
        const auto number = value.get<int64_t>();
        if (number >= INT32_MIN && number <= INT32_MAX) {
            return TypedValue(static_cast<int32_t>(number));
        }
        return TypedValue(number);
    }
    if (value.is_number_float()) {
        return TypedValue(value.get<double>());
    }
    if (value.is_string()) {
        return TypedValue(value.get<std::string>());
    }
    if (value.is_array()) {
        std::list<TypedValue> list;
        for (const auto &item : value) {
            list.push_back(encode(item));
        }
        return TypedValue(list);
    }
    if (value.is_object()) {
        std::unordered_map<std::string, TypedValue> map;
        for (const auto &item : value.items()) {
            map.emplace(item.key(), encode(item.value()));
        }
        return TypedValue(map);
    }
    throw std::invalid_argument("Unsupported parameter value");
}

static json decode(TypedValue value);

static json decode_map(const std::unordered_map<std::string, TypedValue> &values) {
    auto result = json::object();
    for (const auto &[key, value] : values) {
        result[key] = decode(value);
    }
    return result;
}

static json decode_node(const Results::Node &node) {
    return {{"$type", "node"}, {"id", node.get_id()}, {"labels", node.get_labels()}, {"properties", decode_map(node)}};
}

static json decode_edge(const Results::Edge &edge) {
    return {{"$type", "edge"}, {"id", edge.get_id()}, {"labels", edge.get_labels()}, {"properties", decode_map(edge)},
            {"left", edge.get_left()}, {"right", edge.get_right()}};
}

template <typename Graph>
static json decode_graph(const char *type, const Graph &graph) {
    auto nodes = json::array();
    auto edges = json::array();
    for (const auto &node : graph.get_nodes()) {
        nodes.push_back(decode_node(node));
    }
    for (const auto &edge : graph.get_edges()) {
        edges.push_back(decode_edge(edge));
    }
    return {{"$type", type}, {"nodes", nodes}, {"edges", edges}};
}

static json decode_float(double value) {
    if (std::isfinite(value)) {
        return value;
    }
    return {{"$type", "float"}, {"value", std::isnan(value) ? "NaN" : (value > 0 ? "Infinity" : "-Infinity")}};
}

static json decode(TypedValue value) {
    switch (value.get_value_case()) {
        case Value::kNull: return nullptr;
        case Value::kBoolean: return value.as_boolean();
        case Value::kInteger: return value.as_int32_t();
        case Value::kLong: return value.as_int64_t();
        case Value::kString: return value.as_string();
        case Value::kFloat: return decode_float(value.as_float());
        case Value::kDouble: return decode_float(value.as_double());
        case Value::kBigDecimal: return {{"$type", "float"}, {"value", value.as_decimal_string()}};
        case Value::kDocument: return {{"$type", "map"}, {"entries", decode_map(value.as_document())}};
        case Value::kList: {
            auto list = json::array();
            for (const auto &element : value.as_list()) {
                list.push_back(decode(element));
            }
            return list;
        }
        case Value::kNode: return decode_node(value.as_node());
        case Value::kEdge: return decode_edge(value.as_edge());
        case Value::kPath: return decode_graph("path", value.as_path());
        case Value::kGraph: return decode_graph("graph", value.as_graph());
        default: throw std::invalid_argument("Unsupported result value");
    }
}

static json execute_request(Connection::Connection &connection, const json &request) {
    const auto op = request.at("op");
    if (op == "commit") {
        connection.commit();
        return json::object();
    }
    if (op == "rollback") {
        connection.rollback();
        return json::object();
    }
    if (op != "query") {
        throw std::invalid_argument("Unknown adapter operation");
    }

    Connection::Cursor cursor(connection);
    const auto language = request.at("language").get<std::string>();
    const auto statement = request.at("statement").get<std::string>();
    const auto &namespace_name = request.at("namespace");
    const auto &parameters = request.at("params");
    std::unique_ptr<Results::Result> result;
    if (parameters.is_null()) {
        result = namespace_name.is_null() ? cursor.execute(language, statement)
                                          : cursor.execute(language, statement, namespace_name.get<std::string>());
    } else {
        std::unordered_map<std::string, TypedValue> bindings;
        for (const auto &item : parameters.items()) {
            bindings.emplace(item.key(), encode(item.value()));
        }
        if (namespace_name.is_null()) {
            cursor.prepare_named(language, statement);
        } else {
            cursor.prepare_named(language, statement, namespace_name.get<std::string>());
        }
        result = cursor.execute_prepared(bindings);
    }

    auto columns = json::array();
    auto rows = json::array();
    if (result->get_result_type() == Results::ResultType::RELATIONAL) {
        auto relational = result->unwrap<Results::RelationalResult>();
        const auto metadata = relational->get_metadata();
        for (uint32_t i = 0; i < metadata->get_column_count(); ++i) {
            columns.push_back(metadata->get_column_meta(i).get_column_label());
        }
        for (const auto &row : *relational) {
            auto cells = json::array();
            for (const auto &cell : row) {
                cells.push_back(decode(cell));
            }
            rows.push_back(std::move(cells));
        }
    } else if (result->get_result_type() != Results::ResultType::SCALAR) {
        throw std::runtime_error("Unexpected result frame after setting CypherResultFormat");
    }
    result->close();
    return {{"columns", columns}, {"rows", rows}};
}

int main() {
    try {
        Connection::ConnectionProperties config;
        config.set_username(environment("M23_USERNAME"));
        config.set_password(environment("M23_PASSWORD"));
        config.set_is_auto_commit(false);
        const auto port = std::stoul(environment("M23_PORT"));
        if (port == 0 || port > std::numeric_limits<uint16_t>::max()) {
            throw std::out_of_range("Invalid Prism port");
        }
        auto transport = std::make_unique<Transport::PlainTcpTransport>(environment("M23_HOST"), static_cast<uint16_t>(port));
        Connection::Connection connection(config, std::move(transport));
        connection.set_client_info_properties({{"CypherResultFormat", "relational"}});
        for (std::string line; std::getline(std::cin, line); ) {
            json response;
            try {
                response = execute_request(connection, json::parse(line));
            } catch (const Errors::ServerError &error) {
                json details = {{"message", error.what()}};
                if (error.get_sql_state()) {
                    details["code"] = *error.get_sql_state();
                }
                response = {{"error", details}};
            } catch (const std::exception &error) {
                response = {{"error", {{"message", error.what()}}}};
            }
            std::cout << response.dump() << std::endl;
        }
        connection.rollback();
    } catch (const std::exception &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}

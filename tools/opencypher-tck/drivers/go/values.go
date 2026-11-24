package main

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"

	"github.com/polypheny/Polypheny-Go-Driver/prism"
)

func decodeFrame(frame *prism.Frame, out result) (result, error) {
	if frame == nil {
		return out, nil
	}
	if frame.GetRelationalFrame() == nil {
		return out, fmt.Errorf("expected a relational frame after setting CypherResultFormat")
	}
	relational := frame.GetRelationalFrame()
	for _, column := range relational.GetColumnMeta() {
		out.Columns = append(out.Columns, column.GetColumnLabel())
	}
	for _, row := range relational.GetRows() {
		values, err := decodeList(row.GetValues())
		if err != nil {
			return out, err
		}
		out.Rows = append(out.Rows, values)
	}
	return out, nil
}

func encodeMap(object map[string]any) (map[string]*prism.ProtoValue, error) {
	out := make(map[string]*prism.ProtoValue, len(object))
	for key, element := range object {
		value, err := encodeValue(element)
		if err != nil {
			return nil, fmt.Errorf("parameter %q: %w", key, err)
		}
		out[key] = value
	}
	return out, nil
}

func encodeValue(raw any) (*prism.ProtoValue, error) {
	switch value := raw.(type) {
	case nil:
		return &prism.ProtoValue{Value: &prism.ProtoValue_Null{Null: &prism.ProtoNull{}}}, nil
	case bool:
		return &prism.ProtoValue{Value: &prism.ProtoValue_Boolean{Boolean: &prism.ProtoBoolean{Boolean: value}}}, nil
	case string:
		return &prism.ProtoValue{Value: &prism.ProtoValue_String_{String_: &prism.ProtoString{String_: value}}}, nil
	case json.Number:
		if integer, err := value.Int64(); err == nil {
			if integer >= math.MinInt32 && integer <= math.MaxInt32 {
				return &prism.ProtoValue{Value: &prism.ProtoValue_Integer{Integer: &prism.ProtoInteger{Integer: int32(integer)}}}, nil
			}
			return &prism.ProtoValue{Value: &prism.ProtoValue_Long{Long: &prism.ProtoLong{Long: integer}}}, nil
		}
		number, err := value.Float64()
		return &prism.ProtoValue{Value: &prism.ProtoValue_Double{Double: &prism.ProtoDouble{Double: number}}}, err
	case []any:
		values := make([]*prism.ProtoValue, len(value))
		for i, item := range value {
			var err error
			values[i], err = encodeValue(item)
			if err != nil {
				return nil, err
			}
		}
		return &prism.ProtoValue{Value: &prism.ProtoValue_List{List: &prism.ProtoList{Values: values}}}, nil
	case map[string]any:
		entries, err := encodeMap(value)
		return &prism.ProtoValue{Value: &prism.ProtoValue_Document{Document: &prism.ProtoDocument{Entries: entries}}}, err
	default:
		return nil, fmt.Errorf("unsupported parameter value %T", value)
	}
}

func decodeValue(raw *prism.ProtoValue) (any, error) {
	switch value := raw.GetValue().(type) {
	case *prism.ProtoValue_Null:
		return nil, nil
	case *prism.ProtoValue_Boolean:
		return value.Boolean.GetBoolean(), nil
	case *prism.ProtoValue_Integer:
		return value.Integer.GetInteger(), nil
	case *prism.ProtoValue_Long:
		return value.Long.GetLong(), nil
	case *prism.ProtoValue_Float:
		return jsonFloat(float64(value.Float.GetFloat())), nil
	case *prism.ProtoValue_Double:
		return jsonFloat(value.Double.GetDouble()), nil
	case *prism.ProtoValue_BigDecimal:
		data := value.BigDecimal.GetUnscaledValue()
		integer := new(big.Int).SetBytes(data)
		if len(data) > 0 && data[0]&0x80 != 0 {
			integer.Sub(integer, new(big.Int).Lsh(big.NewInt(1), uint(len(data)*8)))
		}
		return map[string]string{"$type": "float", "value": fmt.Sprintf("%se%d", integer, -int64(value.BigDecimal.GetScale()))}, nil
	case *prism.ProtoValue_String_:
		return value.String_.GetString_(), nil
	case *prism.ProtoValue_List:
		return decodeList(value.List.GetValues())
	case *prism.ProtoValue_Document:
		entries, err := decodeMap(value.Document.GetEntries())
		return map[string]any{"$type": "map", "entries": entries}, err
	case *prism.ProtoValue_Node:
		return decodeNode(value.Node)
	case *prism.ProtoValue_Edge:
		return decodeEdge(value.Edge)
	case *prism.ProtoValue_Path:
		return decodeGraph("path", value.Path.GetNodes(), value.Path.GetEdges())
	case *prism.ProtoValue_Graph:
		return decodeGraph("graph", value.Graph.GetNodes(), value.Graph.GetEdges())
	default:
		return nil, fmt.Errorf("unsupported Prism result value %T", value)
	}
}

func jsonFloat(number float64) any {
	var name string
	switch {
	case math.IsNaN(number):
		name = "NaN"
	case math.IsInf(number, 1):
		name = "Infinity"
	case math.IsInf(number, -1):
		name = "-Infinity"
	default:
		name = strconv.FormatFloat(number, 'g', -1, 64)
	}
	return map[string]string{"$type": "float", "value": name}
}

func decodeList(list []*prism.ProtoValue) ([]any, error) {
	out := make([]any, len(list))
	for i, element := range list {
		var err error
		out[i], err = decodeValue(element)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func decodeMap(object map[string]*prism.ProtoValue) (map[string]any, error) {
	out := make(map[string]any, len(object))
	for key, element := range object {
		value, err := decodeValue(element)
		if err != nil {
			return nil, err
		}
		out[key] = value
	}
	return out, nil
}

func decodeNode(node *prism.ProtoNode) (map[string]any, error) {
	properties, err := decodeMap(node.GetProperties())
	return map[string]any{
		"$type": "node", "id": node.GetId(), "labels": append([]string{}, node.GetLabels()...), "properties": properties,
	}, err
}

func decodeEdge(edge *prism.ProtoEdge) (map[string]any, error) {
	properties, err := decodeMap(edge.GetProperties())
	return map[string]any{
		"$type": "edge", "id": edge.GetId(), "labels": append([]string{}, edge.GetLabels()...), "properties": properties,
		"left": edge.GetLeft(), "right": edge.GetRight(),
	}, err
}

func decodeGraph(kind string, nodes []*prism.ProtoNode, edges []*prism.ProtoEdge) (map[string]any, error) {
	decodedNodes := make([]map[string]any, len(nodes))
	decodedEdges := make([]map[string]any, len(edges))
	for i, node := range nodes {
		var err error
		decodedNodes[i], err = decodeNode(node)
		if err != nil {
			return nil, err
		}
	}
	for i, edge := range edges {
		var err error
		decodedEdges[i], err = decodeEdge(edge)
		if err != nil {
			return nil, err
		}
	}
	return map[string]any{"$type": kind, "nodes": decodedNodes, "edges": decodedEdges}, nil
}

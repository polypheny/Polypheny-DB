package main

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	polypheny "github.com/polypheny/Polypheny-Go-Driver"
	"github.com/polypheny/Polypheny-Go-Driver/prism"
)

type request struct {
	Op        string         `json:"op"`
	Language  string         `json:"language"`
	Namespace string         `json:"namespace"`
	Statement string         `json:"statement"`
	Params    map[string]any `json:"params"`
}

type result struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	requestTimeout := 60 * time.Second
	if configured := os.Getenv("M23_REQUEST_TIMEOUT_SECONDS"); configured != "" {
		seconds, err := strconv.Atoi(configured)
		if err != nil || seconds < 1 || seconds > 600 {
			return errors.New("M23_REQUEST_TIMEOUT_SECONDS must be an integer between 1 and 600")
		}
		requestTimeout = time.Duration(seconds) * time.Second
	}
	address := net.JoinHostPort(os.Getenv("M23_HOST"), os.Getenv("M23_PORT"))
	connector, err := (polypheny.PolyphenyDriver{}).OpenConnector(address + "," + os.Getenv("M23_USERNAME") + ":" + os.Getenv("M23_PASSWORD"))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	raw, err := connector.Connect(ctx)
	if err != nil {
		cancel()
		return err
	}
	conn := raw.(*polypheny.PolyphenyConn)
	defer conn.Close()
	err = conn.SetClientInfoPropertiesContext(ctx, map[string]string{"CypherResultFormat": "relational"})
	cancel()
	if err != nil {
		return err
	}

	var tx driver.Tx
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()
	decoder := json.NewDecoder(os.Stdin)
	decoder.UseNumber()
	encoder := json.NewEncoder(os.Stdout)
	for {
		var req request
		if err := decoder.Decode(&req); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		response, queryErr := handleRequest(ctx, conn, &tx, req)
		cancel()
		if queryErr != nil {
			info := map[string]string{"message": queryErr.Error()}
			var clientErr *polypheny.ClientError
			if errors.As(queryErr, &clientErr) && clientErr.SQLState() != "" {
				info["code"] = clientErr.SQLState()
			}
			response = map[string]any{"error": info}
		}
		if err := encoder.Encode(response); err != nil {
			return err
		}
	}
}

func handleRequest(ctx context.Context, conn *polypheny.PolyphenyConn, tx *driver.Tx, req request) (any, error) {
	switch req.Op {
	case "commit", "rollback":
		if *tx == nil {
			return struct{}{}, nil
		}
		previous := *tx
		*tx = nil
		if req.Op == "commit" {
			return struct{}{}, previous.Commit()
		}
		return struct{}{}, previous.Rollback()
	case "query":
		if *tx == nil {
			var err error
			*tx, err = conn.BeginTx(ctx, driver.TxOptions{})
			if err != nil {
				return nil, err
			}
		}
		return query(ctx, conn, req)
	default:
		return nil, fmt.Errorf("unknown operation %q", req.Op)
	}
}

func query(ctx context.Context, conn *polypheny.PolyphenyConn, req request) (result, error) {
	out := result{Columns: []string{}, Rows: [][]any{}}
	switch req.Language {
	case "cypher":
		var params map[string]*prism.ProtoValue
		if req.Params != nil {
			var err error
			params, err = encodeMap(req.Params)
			if err != nil {
				return out, err
			}
		}
		response, err := conn.ExecuteCypherWithParametersContext(ctx, req.Namespace, req.Statement, params)
		if err != nil {
			return out, err
		}
		return decodeFrame(response.GetFrame(), out)
	case "sql":
		if req.Params != nil || req.Namespace != "" {
			return out, errors.New("SQL fixtures require a statement in the default namespace without parameters")
		}
		return sqlQuery(ctx, conn, req.Statement, out)
	default:
		return out, fmt.Errorf("unknown language %q", req.Language)
	}
}

func sqlQuery(ctx context.Context, conn *polypheny.PolyphenyConn, statement string, out result) (result, error) {
	words := strings.Fields(statement)
	if len(words) == 0 {
		return out, errors.New("empty SQL statement")
	}
	switch strings.ToUpper(words[0]) {
	case "SELECT", "WITH", "VALUES":
		rows, err := conn.QueryContext(ctx, statement, nil)
		if err != nil {
			return out, err
		}
		defer rows.Close()
		out.Columns = rows.Columns()
		for {
			values := make([]driver.Value, len(out.Columns))
			err := rows.Next(values)
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			if err != nil {
				return out, err
			}
			row := make([]any, len(values))
			for i, value := range values {
				row[i] = value
			}
			out.Rows = append(out.Rows, row)
		}
	default:
		_, err := conn.ExecContext(ctx, statement, nil)
		return out, err
	}
}

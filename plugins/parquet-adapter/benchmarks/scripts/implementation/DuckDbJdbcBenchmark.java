import java.io.BufferedWriter;
import java.io.IOException;
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

public class DuckDbJdbcBenchmark {

    private record Query( String id, String description, String sql ) {
    }


    private record Config(
            String url,
            Path dataDir,
            Path queries,
            Path output,
            int warmups,
            int runs,
            boolean printRows,
            int fetchSize,
            int queryTimeoutSeconds,
            int threads,
            String memoryLimit,
            Path nestedCustomerFile,
            String sql,
            Path sqlFile,
            Set<String> only ) {
    }


    public static void main( String[] args ) throws Exception {
        Config config = parseArgs( args );
        String adHocSql = adHocSql( config );
        List<Query> queries = adHocSql.isBlank()
                ? loadQueries( config.queries() )
                : List.of( new Query( "SQL", "Ad hoc SQL", adHocSql ) );
        if ( !config.only().isEmpty() ) {
            queries = queries.stream()
                    .filter( query -> config.only().contains( query.id().toUpperCase( Locale.ROOT ) ) )
                    .toList();
        }
        if ( queries.isEmpty() ) {
            throw new IllegalArgumentException( "No queries found in " + config.queries() );
        }

        Class.forName( "org.duckdb.DuckDBDriver" );

        if ( config.output().getParent() != null ) {
            Files.createDirectories( config.output().getParent() );
        }

        try ( Connection connection = DriverManager.getConnection( config.url() );
                BufferedWriter writer = Files.newBufferedWriter( config.output(), StandardCharsets.UTF_8 ) ) {
            setupDuckDb( connection, config );
            writer.write( "timestamp,query_id,description,phase,run,elapsed_ms,rows,columns,success,error" );
            writer.newLine();
            writer.flush();

            System.out.printf( "Connected to %s%n", config.url() );
            System.out.printf( "Data directory %s%n", config.dataDir() );
            if ( adHocSql.isBlank() ) {
                System.out.printf( "Loaded %d queries from %s%n", queries.size(), config.queries() );
            } else {
                System.out.printf( "Loaded ad hoc SQL query%n" );
            }
            System.out.printf( "Writing CSV to %s%n", config.output() );

            for ( Query query : queries ) {
                System.out.printf( "%n%s %s%n", query.id(), query.description() );
                for ( int i = 1; i <= config.warmups(); i++ ) {
                    executeAndRecord( connection, writer, query, "warmup", i, config );
                }
                for ( int i = 1; i <= config.runs(); i++ ) {
                    executeAndRecord( connection, writer, query, "measured", i, config );
                }
            }
        }
    }


    private static void setupDuckDb( Connection connection, Config config ) throws SQLException {
        try ( Statement statement = connection.createStatement() ) {
            statement.execute( "SET memory_limit = '" + config.memoryLimit().replace( "'", "''" ) + "'" );
            statement.execute( "SET threads = " + config.threads() );
            if ( config.nestedCustomerFile() != null ) {
                String file = duckDbPath( config.nestedCustomerFile() );
                statement.execute( "CREATE OR REPLACE VIEW nested_customer AS SELECT * FROM read_parquet('" + file + "')" );
                return;
            }
            String dataDir = duckDbPath( config.dataDir() );
            statement.execute( "CREATE OR REPLACE VIEW yellow_tripdata AS SELECT * FROM read_parquet('" + dataDir + "/yellow_tripdata/**/*.parquet', hive_partitioning = true)" );
            statement.execute( "CREATE OR REPLACE VIEW green_tripdata AS SELECT * FROM read_parquet('" + dataDir + "/green_tripdata/**/*.parquet', hive_partitioning = true)" );
            statement.execute( "CREATE OR REPLACE VIEW fhv_tripdata AS SELECT * FROM read_parquet('" + dataDir + "/fhv_tripdata/**/*.parquet', hive_partitioning = true)" );
            statement.execute( "CREATE OR REPLACE VIEW fhvhv_tripdata AS SELECT * FROM read_parquet('" + dataDir + "/fhvhv_tripdata/**/*.parquet', hive_partitioning = true)" );
        }
    }


    private static String duckDbPath( Path path ) {
        return path.toAbsolutePath().normalize().toString().replace( "\\", "/" ).replace( "'", "''" );
    }


    private static String adHocSql( Config config ) throws IOException {
        if ( config.sqlFile() != null ) {
            return Files.readString( config.sqlFile(), StandardCharsets.UTF_8 );
        }
        return config.sql();
    }


    private static void executeAndRecord( Connection connection, BufferedWriter writer, Query query, String phase, int run, Config config ) throws IOException {
        long start = System.nanoTime();
        long rows = 0;
        int columns = 0;
        boolean success = false;
        String error = "";

        try ( Statement statement = connection.createStatement() ) {
            if ( config.fetchSize() > 0 ) {
                statement.setFetchSize( config.fetchSize() );
            }
            if ( config.queryTimeoutSeconds() > 0 ) {
                statement.setQueryTimeout( config.queryTimeoutSeconds() );
            }

            boolean hasResultSet = statement.execute( query.sql() );
            if ( hasResultSet ) {
                try ( ResultSet resultSet = statement.getResultSet() ) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    columns = metaData.getColumnCount();
                    while ( resultSet.next() ) {
                        rows++;
                        if ( config.printRows() ) {
                            printRow( resultSet, metaData );
                        }
                    }
                }
            } else {
                rows = statement.getUpdateCount();
            }
            success = true;
        } catch ( SQLException e ) {
            error = e.getClass().getSimpleName() + ": " + e.getMessage();
            System.out.printf( "  %s %d failed: %s%n", phase, run, error );
        }

        long elapsedMs = Math.round( (System.nanoTime() - start) / 1_000_000.0 );
        if ( success ) {
            System.out.printf( "  %s %d: %d ms, rows=%d%n", phase, run, elapsedMs, rows );
        }
        writeCsvRow( writer, query, phase, run, elapsedMs, rows, columns, success, error );
    }


    private static void printRow( ResultSet resultSet, ResultSetMetaData metaData ) throws SQLException {
        List<String> values = new ArrayList<>();
        for ( int i = 1; i <= metaData.getColumnCount(); i++ ) {
            Object value = resultSet.getObject( i );
            values.add( value == null ? "NULL" : value.toString() );
        }
        System.out.println( String.join( "\t", values ) );
    }


    private static void writeCsvRow( BufferedWriter writer, Query query, String phase, int run, long elapsedMs, long rows, int columns, boolean success, String error ) throws IOException {
        writer.write( String.join( ",",
                csv( Instant.now().toString() ),
                csv( query.id() ),
                csv( query.description() ),
                csv( phase ),
                Integer.toString( run ),
                Long.toString( elapsedMs ),
                Long.toString( rows ),
                Integer.toString( columns ),
                Boolean.toString( success ),
                csv( error ) ) );
        writer.newLine();
        writer.flush();
    }


    private static String csv( String value ) {
        String escaped = value == null ? "" : value.replace( "\"", "\"\"" );
        return "\"" + escaped + "\"";
    }


    private static List<Query> loadQueries( Path path ) throws IOException {
        List<String> lines = Files.readAllLines( path, StandardCharsets.UTF_8 );
        List<Query> queries = new ArrayList<>();
        String currentId = null;
        String currentDescription = null;
        StringBuilder sql = new StringBuilder();

        for ( String line : lines ) {
            String trimmed = line.trim();
            if ( trimmed.startsWith( "-- Q" ) && trimmed.contains( ":" ) ) {
                addQuery( queries, currentId, currentDescription, sql );
                int colon = trimmed.indexOf( ':' );
                currentId = trimmed.substring( 3, colon ).trim().toUpperCase( Locale.ROOT );
                currentDescription = trimmed.substring( colon + 1 ).trim();
                sql.setLength( 0 );
                continue;
            }
            if ( trimmed.startsWith( "--" ) || trimmed.isEmpty() ) {
                continue;
            }
            sql.append( line ).append( System.lineSeparator() );
            if ( trimmed.endsWith( ";" ) ) {
                addQuery( queries, currentId, currentDescription, sql );
                currentId = null;
                currentDescription = null;
                sql.setLength( 0 );
            }
        }
        addQuery( queries, currentId, currentDescription, sql );
        return queries;
    }


    private static void addQuery( List<Query> queries, String id, String description, StringBuilder sql ) {
        String statement = sql.toString().trim();
        if ( statement.isEmpty() ) {
            return;
        }
        if ( statement.endsWith( ";" ) ) {
            statement = statement.substring( 0, statement.length() - 1 );
        }
        queries.add( new Query( id == null ? "SQL" : id, description == null ? "" : description, statement ) );
    }


    private static Config parseArgs( String[] args ) {
        Map<String, String> values = new LinkedHashMap<>();
        Set<String> flags = Set.of( "print-rows" );
        for ( int i = 0; i < args.length; i++ ) {
            String arg = args[i];
            if ( !arg.startsWith( "--" ) ) {
                throw new IllegalArgumentException( "Unexpected argument: " + arg );
            }
            String key = arg.substring( 2 );
            if ( flags.contains( key ) ) {
                values.put( key, "true" );
                continue;
            }
            if ( key.contains( "=" ) ) {
                String[] split = key.split( "=", 2 );
                values.put( split[0], split[1] );
                continue;
            }
            if ( i + 1 >= args.length ) {
                throw new IllegalArgumentException( "Missing value for " + arg );
            }
            values.put( key, args[++i] );
        }

        Path sqlFile = values.containsKey( "sql-file" ) && !values.get( "sql-file" ).isBlank()
                ? Path.of( values.get( "sql-file" ) )
                : null;
        Path nestedCustomerFile = values.containsKey( "nested-customer-file" ) && !values.get( "nested-customer-file" ).isBlank()
                ? Path.of( values.get( "nested-customer-file" ) )
                : null;
        String only = values.getOrDefault( "only", "" );
        return new Config(
                values.getOrDefault( "url", "jdbc:duckdb:" ),
                Path.of( values.getOrDefault( "data-dir", "C:\\tmp\\tlc\\flat_partitioned" ) ),
                Path.of( values.getOrDefault( "queries", "plugins/parquet-adapter/benchmarks/query_lists/access_model_comparison/access_model_comparison_sql.sql" ) ),
                Path.of( values.getOrDefault( "output", "plugins/parquet-adapter/benchmarks/results/access_model_comparison/duckdb_results.csv" ) ),
                parseInt( values, "warmups", 1 ),
                parseInt( values, "runs", 5 ),
                parseBoolean( values, "print-rows", false ),
                parseInt( values, "fetch-size", 1000 ),
                parseInt( values, "query-timeout", 0 ),
                parseInt( values, "threads", 8 ),
                values.getOrDefault( "memory-limit", "16GB" ),
                nestedCustomerFile,
                values.getOrDefault( "sql", "" ),
                sqlFile,
                only.isBlank()
                        ? Set.of()
                        : Set.of( only.split( "," ) ).stream().map( s -> s.trim().toUpperCase( Locale.ROOT ) ).collect( Collectors.toSet() ) );
    }


    private static int parseInt( Map<String, String> values, String key, int defaultValue ) {
        return values.containsKey( key ) ? Integer.parseInt( values.get( key ) ) : defaultValue;
    }


    private static boolean parseBoolean( Map<String, String> values, String key, boolean defaultValue ) {
        return values.containsKey( key ) ? Boolean.parseBoolean( values.get( key ) ) : defaultValue;
    }

}

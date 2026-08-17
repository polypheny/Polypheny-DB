import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;


public class TlcUnpartitionedMaterializer {

    private static final Pattern YEAR_FOLDER = Pattern.compile( "year=(\\d{4})" );
    private static final Pattern MONTH_FOLDER = Pattern.compile( "month=(\\d{2})" );

    private record Config(
            Path inputDir,
            Path outputDir,
            int threads,
            String memoryLimit,
            String compression,
            boolean overwrite,
            boolean dryRun,
            boolean verifyOnly ) {
    }


    private record SourceFile( Path source, Path output, String table, String year, String month ) {
    }


    public static void main( String[] args ) throws Exception {
        Config config = parseArgs( args );
        if ( !Files.isDirectory( config.inputDir() ) ) {
            throw new IllegalArgumentException( "Input directory does not exist: " + config.inputDir() );
        }

        List<SourceFile> files = discoverFiles( config );
        if ( files.isEmpty() ) {
            throw new IllegalArgumentException( "No partitioned Parquet files found in " + config.inputDir() );
        }

        Class.forName( "org.duckdb.DuckDBDriver" );
        try ( Connection connection = DriverManager.getConnection( "jdbc:duckdb:" );
                Statement statement = connection.createStatement() ) {
            statement.execute( "PRAGMA threads=" + config.threads() );
            statement.execute( "SET memory_limit=" + sqlString( config.memoryLimit() ) );

            System.out.printf( "Input:       %s%n", config.inputDir() );
            System.out.printf( "Output:      %s%n", config.outputDir() );
            System.out.printf( "Files:       %d%n", files.size() );
            System.out.printf( "Compression: %s%n", config.compression() );
            System.out.printf( "Mode:        %s%n%n", config.verifyOnly() ? "verify" : config.dryRun() ? "dry-run" : "materialize" );

            int skipped = 0;
            int processed = 0;
            Instant allStarted = Instant.now();
            for ( int index = 0; index < files.size(); index++ ) {
                SourceFile file = files.get( index );
                Instant started = Instant.now();
                if ( config.verifyOnly() ) {
                    verifyFile( statement, file );
                    System.out.printf( "[%d/%d] verified %s (%s)%n", index + 1, files.size(), file.output(), elapsed( started ) );
                    processed++;
                    continue;
                }

                if ( Files.exists( file.output() ) && !config.overwrite() ) {
                    System.out.printf( "[%d/%d] skip existing %s%n", index + 1, files.size(), file.output() );
                    skipped++;
                    continue;
                }

                System.out.printf(
                        "[%d/%d] %s year=%s month=%s -> %s%n",
                        index + 1,
                        files.size(),
                        file.table(),
                        file.year(),
                        file.month(),
                        file.output() );
                if ( config.dryRun() ) {
                    processed++;
                    continue;
                }

                Files.createDirectories( file.output().getParent() );
                Path temporary = Path.of( file.output() + ".tmp" );
                if ( Files.exists( temporary ) && !config.overwrite() ) {
                    throw new IllegalStateException( "Temporary output already exists. Remove it or rerun with -Overwrite: " + temporary );
                }
                if ( config.overwrite() ) {
                    Files.deleteIfExists( temporary );
                    Files.deleteIfExists( file.output() );
                }

                materializeFile( statement, file, temporary, config.compression() );
                verifyFile( statement, new SourceFile( file.source(), temporary, file.table(), file.year(), file.month() ) );
                Files.move( temporary, file.output(), StandardCopyOption.ATOMIC_MOVE );
                System.out.printf( "        completed in %s%n", elapsed( started ) );
                processed++;
            }

            System.out.printf(
                    "%nDone. Processed=%d skipped=%d total=%d elapsed=%s%n",
                    processed,
                    skipped,
                    files.size(),
                    elapsed( allStarted ) );
        }
    }


    private static List<SourceFile> discoverFiles( Config config ) throws Exception {
        List<SourceFile> files = new ArrayList<>();
        Map<Path, Path> outputOwners = new LinkedHashMap<>();
        try ( Stream<Path> stream = Files.walk( config.inputDir() ) ) {
            for ( Path source : stream.filter( Files::isRegularFile ).filter( TlcUnpartitionedMaterializer::isParquet ).sorted().toList() ) {
                Path relative = config.inputDir().relativize( source );
                if ( relative.getNameCount() != 4 ) {
                    throw new IllegalArgumentException( "Expected <table>/year=YYYY/month=MM/<file>.parquet but found: " + relative );
                }

                String table = relative.getName( 0 ).toString();
                String year = partitionValue( YEAR_FOLDER, relative.getName( 1 ).toString(), relative );
                String month = partitionValue( MONTH_FOLDER, relative.getName( 2 ).toString(), relative );
                Path output = config.outputDir().resolve( table ).resolve( relative.getFileName().toString() );
                Path previous = outputOwners.putIfAbsent( output, source );
                if ( previous != null ) {
                    throw new IllegalArgumentException( "Output file collision for " + previous + " and " + source + ": " + output );
                }
                files.add( new SourceFile( source, output, table, year, month ) );
            }
        }
        files.sort( Comparator.comparing( SourceFile::source ) );
        return files;
    }


    private static void materializeFile( Statement statement, SourceFile file, Path temporary, String compression ) throws SQLException {
        String sql = """
                COPY (
                  SELECT
                    *,
                    CAST(%s AS VARCHAR) AS "year",
                    CAST(%s AS VARCHAR) AS "month"
                  FROM read_parquet(%s, hive_partitioning = false)
                ) TO %s (FORMAT PARQUET, COMPRESSION %s)
                """.formatted(
                sqlString( file.year() ),
                sqlString( file.month() ),
                sqlString( file.source() ),
                sqlString( temporary ),
                compression );
        statement.execute( sql );
    }


    private static void verifyFile( Statement statement, SourceFile file ) throws SQLException {
        if ( !Files.isRegularFile( file.output() ) ) {
            throw new IllegalStateException( "Output file does not exist: " + file.output() );
        }

        long sourceRows = parquetRows( statement, file.source() );
        long outputRows = parquetRows( statement, file.output() );
        if ( sourceRows != outputRows ) {
            throw new IllegalStateException( "Row-count mismatch for " + file.output() + ": source=" + sourceRows + " output=" + outputRows );
        }

        String sql = """
                SELECT
                  count(DISTINCT row_group_id) AS row_groups,
                  count(*) FILTER (WHERE path_in_schema = 'year') AS year_stats,
                  count(*) FILTER (WHERE path_in_schema = 'month') AS month_stats,
                  count(*) FILTER (
                    WHERE path_in_schema = 'year'
                      AND (stats_min <> %s OR stats_max <> %s OR stats_null_count <> 0)
                  ) AS bad_year,
                  count(*) FILTER (
                    WHERE path_in_schema = 'month'
                      AND (stats_min <> %s OR stats_max <> %s OR stats_null_count <> 0)
                  ) AS bad_month
                FROM parquet_metadata(%s)
                """.formatted(
                sqlString( file.year() ),
                sqlString( file.year() ),
                sqlString( file.month() ),
                sqlString( file.month() ),
                sqlString( file.output() ) );
        try ( ResultSet result = statement.executeQuery( sql ) ) {
            result.next();
            long rowGroups = result.getLong( "row_groups" );
            if ( rowGroups == 0
                    || result.getLong( "year_stats" ) != rowGroups
                    || result.getLong( "month_stats" ) != rowGroups
                    || result.getLong( "bad_year" ) != 0
                    || result.getLong( "bad_month" ) != 0 ) {
                throw new IllegalStateException( "Physical partition-column verification failed for " + file.output() );
            }
        }
    }


    private static long parquetRows( Statement statement, Path file ) throws SQLException {
        try ( ResultSet result = statement.executeQuery( "SELECT sum(num_rows) FROM parquet_file_metadata(" + sqlString( file ) + ")" ) ) {
            result.next();
            return result.getLong( 1 );
        }
    }


    private static String partitionValue( Pattern pattern, String folder, Path relative ) {
        Matcher matcher = pattern.matcher( folder );
        if ( !matcher.matches() ) {
            throw new IllegalArgumentException( "Unexpected partition folder in " + relative + ": " + folder );
        }
        return matcher.group( 1 );
    }


    private static boolean isParquet( Path path ) {
        return path.getFileName().toString().toLowerCase( Locale.ROOT ).endsWith( ".parquet" );
    }


    private static String elapsed( Instant started ) {
        Duration duration = Duration.between( started, Instant.now() );
        long seconds = duration.toSeconds();
        return "%dm %02ds".formatted( seconds / 60, seconds % 60 );
    }


    private static String sqlString( Object value ) {
        return "'" + value.toString().replace( "\\", "/" ).replace( "'", "''" ) + "'";
    }


    private static Config parseArgs( String[] args ) {
        Map<String, String> values = new LinkedHashMap<>();
        boolean overwrite = false;
        boolean dryRun = false;
        boolean verifyOnly = false;

        for ( int i = 0; i < args.length; i++ ) {
            switch ( args[i] ) {
                case "--overwrite" -> overwrite = true;
                case "--dry-run" -> dryRun = true;
                case "--verify-only" -> verifyOnly = true;
                default -> {
                    if ( !args[i].startsWith( "--" ) || i + 1 >= args.length ) {
                        throw new IllegalArgumentException( "Unexpected argument: " + args[i] );
                    }
                    values.put( args[i].substring( 2 ), args[++i] );
                }
            }
        }

        String compression = values.getOrDefault( "compression", "SNAPPY" ).toUpperCase( Locale.ROOT );
        if ( !List.of( "SNAPPY", "ZSTD", "GZIP", "UNCOMPRESSED" ).contains( compression ) ) {
            throw new IllegalArgumentException( "Unsupported compression: " + compression );
        }
        return new Config(
                Path.of( values.getOrDefault( "input-dir", "C:\\PolyData\\tlc_partitioned" ) ),
                Path.of( values.getOrDefault( "output-dir", "C:\\PolyData\\tlc_unpartitioned" ) ),
                Integer.parseInt( values.getOrDefault( "threads", "8" ) ),
                values.getOrDefault( "memory-limit", "16GB" ),
                compression,
                overwrite,
                dryRun,
                verifyOnly );
    }

}

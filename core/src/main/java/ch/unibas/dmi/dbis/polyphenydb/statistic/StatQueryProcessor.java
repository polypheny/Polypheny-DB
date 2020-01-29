package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.Authenticator;
import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.SqlProcessor;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.TransactionException;
import ch.unibas.dmi.dbis.polyphenydb.TransactionManager;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownDatabaseException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownUserException;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigManager;
import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbSignature;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelRoot;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser.SqlParserConfig;
import ch.unibas.dmi.dbis.polyphenydb.util.LimitIterator;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.linq4j.Enumerable;


@Slf4j
public class StatQueryProcessor {

    private final TransactionManager transactionManager;
    private final String databaseName;
    private final String userName;


    /**
     * LowCostQueries can be used to retrieve short answered queries
     * Idea is to expose a selected list of sql operations with a small list of results and not impact performance
     */
    public StatQueryProcessor( final TransactionManager transactionManager, String userName, String databaseName ) {
        this.transactionManager = transactionManager;
        this.databaseName = databaseName;
        this.userName = userName;
    }


    public StatQueryProcessor( TransactionManager transactionManager, Authenticator authenticator ) {
        this( transactionManager, "pa", "APP" );
    }


    /**
     * Handles the request for one columns stats
     *
     * @param query the select query
     * @return result of the query
     */
    public StatQueryColumn selectOneStat( String query ) {
        StatResult res = this.executeSqlSelect( query );
        if(res.getColumns() != null && res.getColumns().length == 1 ){
            return res.getColumns()[0];
        }
        return null;
    }


    /**
     * Handles the request which retrieves the stats for multiple columns
     */
    public StatResult selectMultipleStats( String query ) {
        return this.executeSqlSelect( query );
    }


    /**
     * Method checks is limit is the same as buffer for statistics
     * adds/matches is to the value in the configManager
     *
     * @param query the query to check
     * @return the "sanitized" query
     */
    private String queryLimitCheck( String query ) {
        Pattern pattern = Pattern.compile( "^.*(?=(limit))" );
        return pattern.matcher( query ) + " LIMIT " + ConfigManager.getInstance().getConfig( "StatisticSize" ) + ";";
    }


    /**
     * Method to get all schemas, tables, and their columns in a database
     * TODO: separate so you can get all or specific database or table
     */
    public ArrayList<ArrayList<String>> getSchemaTree() {

        ArrayList<ArrayList<String>> result = new ArrayList<>();

        Transaction transaction = getTransaction();
        try {

            CatalogDatabase catalogDatabase = transaction.getCatalog().getDatabase( databaseName );
            CatalogCombinedDatabase combinedDatabase = transaction.getCatalog().getCombinedDatabase( catalogDatabase.id );
            ArrayList<String> schemaTree = new ArrayList<>();
            for ( CatalogCombinedSchema combinedSchema : combinedDatabase.getSchemas() ) {
                // schema
                // schemaTree.add( combinedSchema.getSchema().name );

                ArrayList<String> tables = new ArrayList<>();

                for ( CatalogCombinedTable combinedTable : combinedSchema.getTables() ) {
                    // table
                    //table.add( combinedSchema.getSchema().name + "." + combinedTable.getTable().name );

                    ArrayList<String> table = new ArrayList<>();

                    for ( CatalogColumn catalogColumn : combinedTable.getColumns() ) {
                        table.add( combinedSchema.getSchema().name + "." + combinedTable.getTable().name + "." + catalogColumn.name );
                    }

                    if ( combinedTable.getTable().tableType.equals( "TABLE" ) ) {
                        tables.addAll( table );
                    }
                }

                schemaTree.addAll( tables );

                result.add( schemaTree );
            }
            transaction.commit();
        } catch ( UnknownDatabaseException | UnknownTableException | UnknownSchemaException | GenericCatalogException | TransactionException e ) {
            log.error( "Caught exception", e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
        }

        return result;
    }


    /**
     * Gets all columns in the database
     *
     * @return all the columns
     */
    public ArrayList<QueryColumn> getAllColumns() {
        Transaction transaction = getTransaction();

        ArrayList<QueryColumn> columns = new ArrayList<>();

        try {
            CatalogDatabase catalogDatabase = transaction.getCatalog().getDatabase( databaseName );
            CatalogCombinedDatabase combinedDatabase = transaction.getCatalog().getCombinedDatabase( catalogDatabase.id );

            for ( CatalogCombinedSchema schema : combinedDatabase.getSchemas() ) {
                for ( CatalogCombinedTable table : schema.getTables() ) {
                    //TODO: solve, better solution to distinguish between schema, table, column
                    columns.addAll( table.getColumns().stream().map( c -> new QueryColumn( schema.getSchema().name, table.getTable().name, c.name, c.type ) ).collect( Collectors.toList() ) );
                }
            }
            transaction.commit();

        } catch ( UnknownDatabaseException | UnknownTableException | UnknownSchemaException | GenericCatalogException | TransactionException e ) {
            log.error( "Caught exception", e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
        }
        return columns;
    }


    private ArrayList<QueryColumn> getAllColumns( String schemaTable ) {
        String[] split = schemaTable.split( "." );
        if ( split.length != 2 ) {
            return new ArrayList<QueryColumn>();
        }
        return getAllColumns( split[0], split[1] );
    }


    /**
     * Get all columns of a specific table
     *
     * @param schemaName the name of the schema
     * @param tableName the name of the table
     * @return all columns
     */
    ArrayList<QueryColumn> getAllColumns( String schemaName, String tableName ) {
        System.out.println( schemaName );
        Transaction transaction = getTransaction();

        ArrayList<QueryColumn> columns = new ArrayList<>();

        try {
            CatalogDatabase catalogDatabase = transaction.getCatalog().getDatabase( databaseName );
            CatalogCombinedDatabase combinedDatabase = transaction.getCatalog().getCombinedDatabase( catalogDatabase.id );
            for ( CatalogCombinedSchema schema : combinedDatabase.getSchemas() ) {
                if ( schema.getSchema().name.equals( schemaName ) ) {
                    for ( CatalogCombinedTable table : schema.getTables() ) {
                        if ( table.getTable().name.equals( tableName ) ) {
                            columns.addAll( table.getColumns().stream().map( c -> new QueryColumn(schema.getSchema().name, table.getTable().name, c.name, c.type) ).collect( Collectors.toList() ) );
                        }
                    }
                }
            }
            transaction.commit();

        } catch ( UnknownDatabaseException | UnknownTableException | UnknownSchemaException | GenericCatalogException | TransactionException e ) {
            log.error( "Caught exception", e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
        }
        return columns;

    }


    public PolySqlType getColumnType( String schemaTableColumn ) {
        // TODO:
        String[] splits = schemaTableColumn.split( "\\." );
        return getColumnType( splits[0], splits[1], splits[2] );
    }


    /**
     * Method to get the type of a specific column
     */
    public PolySqlType getColumnType( String schema, String table, String column ) {
        Transaction transaction = getTransaction();
        // TODO: fix possible nullpointer
        PolySqlType type = null;

        try {
            CatalogDatabase catalogDatabase = transaction.getCatalog().getDatabase( databaseName );
            CatalogCombinedDatabase combinedDatabase = transaction.getCatalog().getCombinedDatabase( catalogDatabase.id );
            type = combinedDatabase
                    .getSchemas().stream().filter( s -> s.getSchema().name.equals( schema ) ).findFirst().get()
                    .getTables().stream().filter( t -> t.getTable().name.equals( table ) ).findFirst().get()
                    .getColumns().stream().filter( c -> c.name.equals( column ) ).findFirst().get().type;

            transaction.commit();

        } catch ( UnknownDatabaseException | UnknownTableException | UnknownSchemaException | GenericCatalogException | TransactionException e ) {
            log.error( "Caught exception", e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
        }
        return type;

    }


    private StatResult executeSqlSelect( String query ) {
        Transaction transaction = getTransaction();
        StatResult result = new StatResult();
        try {

            result = executeSqlSelect( transaction, query );
            transaction.commit();

        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while executing a query from the console", e );

            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
        }
        return result;
    }


    private Transaction getTransaction() {
        try {
            return transactionManager.startTransaction( userName, databaseName, false );
        } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }
    }

    // -----------------------------------------------------------------------
    //                                Helper
    // -----------------------------------------------------------------------


    private StatResult executeSqlSelect( final Transaction transaction, final String sqlSelect ) throws QueryExecutionException {
        // Parser Config
        SqlParser.ConfigBuilder configConfigBuilder = SqlParser.configBuilder();
        configConfigBuilder.setCaseSensitive( RuntimeConfig.CASE_SENSITIVE.getBoolean() );
        configConfigBuilder.setUnquotedCasing( Casing.TO_LOWER );
        configConfigBuilder.setQuotedCasing( Casing.TO_LOWER );
        SqlParserConfig parserConfig = configConfigBuilder.build();

        PolyphenyDbSignature signature;
        List<List<Object>> rows;
        Iterator<Object> iterator = null;


        try {
            signature = processQuery( transaction, sqlSelect, parserConfig );
            final Enumerable enumerable = signature.enumerable( transaction.getDataContext() );
            //noinspection unchecked

            iterator = enumerable.iterator();

            rows = MetaImpl.collect( signature.cursorFactory, LimitIterator.of( iterator, getPageSize() ), new ArrayList<>() );

        } catch ( Throwable t ) {
            if ( iterator != null ) {
                try {
                    ((AutoCloseable) iterator).close();
                } catch ( Exception e ) {
                    log.error( "Exception while closing result iterator", e );
                }
            }
            throw new QueryExecutionException( t );
        }

        try {
            ArrayList<PolySqlType> types = new ArrayList<>();
            ArrayList<String> names = new ArrayList<>();
            for ( ColumnMetaData metaData : signature.columns ) {

                types.add( PolySqlType.getPolySqlTypeFromSting( metaData.type.name ) );
                // TODO: reevaluate
                names.add( metaData.schemaName + "." + metaData.tableName + "." + metaData.columnName );
            }

            ArrayList<String[]> data = new ArrayList<>();
            for ( List<Object> row : rows ) {
                String[] temp = new String[row.size()];
                int counter = 0;
                for ( Object o : row ) {
                    if ( o == null ) {
                        temp[counter] = null;
                    } else {
                        temp[counter] = o.toString();
                    }
                    counter++;
                }
                data.add( temp );
            }

            // TODO: own result object?
            String[][] d = data.toArray( new String[0][] );

            StatResult res = new StatResult( names, types, d );
            return res;
        } finally {
            try {
                ((AutoCloseable) iterator).close();
            } catch ( Exception e ) {
                log.error( "Exception while closing result iterator2", e );
            }
        }
    }


    private PolyphenyDbSignature processQuery( Transaction transaction, String sql, SqlParserConfig parserConfig ) {
        PolyphenyDbSignature signature;
        transaction.resetQueryProcessor();
        SqlProcessor sqlProcessor = transaction.getSqlProcessor( parserConfig );

        SqlNode parsed = sqlProcessor.parse( sql );

        if ( parsed.isA( SqlKind.DDL ) ) {
            signature = sqlProcessor.prepareDdl( parsed );
        } else {
            Pair<SqlNode, RelDataType> validated = sqlProcessor.validate( parsed );
            RelRoot logicalRoot = sqlProcessor.translate( validated.left );

            // Prepare
            signature = transaction.getQueryProcessor().prepareQuery( logicalRoot );
        }
        return signature;
    }


    /**
     * Get the page
     */
    private int getPageSize() {
        return ConfigManager.getInstance().getConfig( "pageSize" ).getInt();
    }


    public boolean hasData( String schema, String table, String column ) {
        String query = "SELECT * FROM " + buildName( schema, table ) + " LIMIT 1";
        StatResult res = executeSqlSelect( query );
        return res.getColumns().length > 0;
    }


    public static String buildName( String... strings ) {
        return new StringBuilder( "\"" )
                .append( String.join( "\".\"", strings ) )
                .append( "\"" ).toString();
    }



    static class QueryExecutionException extends Exception {

        QueryExecutionException( String message ) {
            super( message );
        }


        QueryExecutionException( String message, Exception e ) {
            super( message, e );
        }


        QueryExecutionException( Throwable t ) {
            super( t );
        }

    }
}

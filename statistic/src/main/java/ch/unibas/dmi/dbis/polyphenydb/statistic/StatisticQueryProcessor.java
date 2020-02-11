/*
 * Copyright 2019-2020 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.Authenticator;
import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.SqlProcessor;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.TransactionException;
import ch.unibas.dmi.dbis.polyphenydb.TransactionManager;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.TableType;
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
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.linq4j.Enumerable;


@Slf4j
public class StatisticQueryProcessor {

    private final TransactionManager transactionManager;
    private final String databaseName;
    private final String userName;


    /**
     * LowCostQueries can be used to retrieve short answered queries
     * Idea is to expose a selected list of sql operations with a small list of results and not impact performance
     */
    public StatisticQueryProcessor( final TransactionManager transactionManager, String userName, String databaseName ) {
        this.transactionManager = transactionManager;
        this.databaseName = databaseName;
        this.userName = userName;
    }


    public StatisticQueryProcessor( TransactionManager transactionManager, Authenticator authenticator ) {
        this( transactionManager, "pa", "APP" );
    }


    /**
     * Handles the request for one columns stats
     *
     * @param query the select query
     * @return result of the query
     */
    public StatisticQueryColumn selectOneStat( String query ) {
        StatisticResult res = this.executeSqlSelect( query );
        if ( res.getColumns() != null && res.getColumns().length == 1 ) {
            return res.getColumns()[0];
        }
        return null;
    }


    /**
     * Handles the request which retrieves the stats for multiple columns
     */
    public StatisticResult selectMultipleStats( String query ) {
        return this.executeSqlSelect( query );
    }


    /**
     * Method to get all schemas, tables, and their columns in a database
     */
    public List<List<String>> getSchemaTree() {
        List<List<String>> result = new ArrayList<>();
        Transaction transaction = getTransaction();

        try {
            CatalogDatabase catalogDatabase = transaction.getCatalog().getDatabase( databaseName );
            CatalogCombinedDatabase combinedDatabase = transaction.getCatalog().getCombinedDatabase( catalogDatabase.id );
            List<String> schemaTree = new ArrayList<>();
            for ( CatalogCombinedSchema combinedSchema : combinedDatabase.getSchemas() ) {
                List<String> tables = new ArrayList<>();
                for ( CatalogCombinedTable combinedTable : combinedSchema.getTables() ) {
                    List<String> table = new ArrayList<>();
                    for ( CatalogColumn catalogColumn : combinedTable.getColumns() ) {
                        table.add( combinedSchema.getSchema().name + "." + combinedTable.getTable().name + "." + catalogColumn.name );
                    }
                    if ( combinedTable.getTable().tableType == TableType.TABLE ) {
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
    public List<QueryColumn> getAllColumns() {
        Transaction transaction = getTransaction();
        List<QueryColumn> columns = new ArrayList<>();

        try {
            CatalogDatabase catalogDatabase = transaction.getCatalog().getDatabase( databaseName );
            CatalogCombinedDatabase combinedDatabase = transaction.getCatalog().getCombinedDatabase( catalogDatabase.id );

            for ( CatalogCombinedSchema schema : combinedDatabase.getSchemas() ) {
                for ( CatalogCombinedTable table : schema.getTables() ) {
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


    private List<QueryColumn> getAllColumns( String schemaTable ) {
        String[] split = schemaTable.split( "." );
        if ( split.length != 2 ) {
            return new ArrayList<>();
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
    public List<QueryColumn> getAllColumns( String schemaName, String tableName ) {
        Transaction transaction = getTransaction();

        List<QueryColumn> columns = new ArrayList<>();

        try {
            CatalogDatabase catalogDatabase = transaction.getCatalog().getDatabase( databaseName );
            CatalogCombinedDatabase combinedDatabase = transaction.getCatalog().getCombinedDatabase( catalogDatabase.id );
            for ( CatalogCombinedSchema schema : combinedDatabase.getSchemas() ) {
                if ( schema.getSchema().name.equals( schemaName ) ) {
                    for ( CatalogCombinedTable table : schema.getTables() ) {
                        if ( table.getTable().name.equals( tableName ) ) {
                            columns.addAll( table.getColumns().stream().map( c -> new QueryColumn( schema.getSchema().name, table.getTable().name, c.name, c.type ) ).collect( Collectors.toList() ) );
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
        String[] splits = schemaTableColumn.split( "\\." );
        if ( splits.length != 3 ) {
            return null;
        }
        return getColumnType( splits[0], splits[1], splits[2] );
    }


    /**
     * Method to get the type of a specific column
     */
    public PolySqlType getColumnType( String schema, String table, String column ) {
        Transaction transaction = getTransaction();
        // TODO: fix possible NullPointer
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


    private StatisticResult executeSqlSelect( String query ) {
        Transaction transaction = getTransaction();
        StatisticResult result = new StatisticResult();
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


    private StatisticResult executeSqlSelect( final Transaction transaction, final String sqlSelect ) throws QueryExecutionException {
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
            List<PolySqlType> types = new ArrayList<>();
            List<String> names = new ArrayList<>();
            for ( ColumnMetaData metaData : signature.columns ) {

                types.add( PolySqlType.getPolySqlTypeFromSting( metaData.type.name ) );
                names.add( metaData.schemaName + "." + metaData.tableName + "." + metaData.columnName );
            }

            List<String[]> data = new ArrayList<>();
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

            String[][] d = data.toArray( new String[0][] );

            return new StatisticResult( names, types, d );
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
        return RuntimeConfig.UI_PAGE_SIZE.getInteger();
    }


    public boolean hasData( String schema, String table, String column ) {
        String query = "SELECT * FROM " + buildQualifiedName( schema, table ) + " LIMIT 1";
        StatisticResult res = executeSqlSelect( query );
        return res.getColumns().length > 0;
    }


    public static String buildQualifiedName( String... strings ) {
        return "\"" + String.join( "\".\"", strings ) + "\"";
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

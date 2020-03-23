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

package org.polypheny.db.statistic;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.linq4j.Enumerable;
import org.polypheny.db.SqlProcessor;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedDatabase;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedSchema;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.parser.SqlParser;
import org.polypheny.db.sql.parser.SqlParser.SqlParserConfig;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.LimitIterator;
import org.polypheny.db.util.Pair;


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


    public PolyType getColumnType( String schemaTableColumn ) {
        String[] splits = schemaTableColumn.split( "\\." );
        if ( splits.length != 3 ) {
            return null;
        }
        return getColumnType( splits[0], splits[1], splits[2] );
    }


    /**
     * Method to get the type of a specific column
     */
    public PolyType getColumnType( String schema, String table, String column ) {
        Transaction transaction = getTransaction();
        // TODO: fix possible NullPointer
        PolyType type = null;

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
            List<PolyType> types = new ArrayList<>();
            List<String> names = new ArrayList<>();
            for ( ColumnMetaData metaData : signature.columns ) {

                types.add( PolyType.get( metaData.type.name ) );
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

/*
 * Copyright 2019-2021 The Polypheny Project
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
import org.polypheny.db.PolyResult;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.core.enums.Kind;
import org.polypheny.db.core.nodes.Node;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.Transaction.MultimediaFlavor;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;


@Slf4j
public class StatisticQueryProcessor {

    private final TransactionManager transactionManager;
    private final String databaseName;
    private final String userName;
    private final Catalog catalog = Catalog.getInstance();


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
        List<String> schemaTree = new ArrayList<>();
        List<CatalogSchema> schemas = catalog.getSchemas( new Pattern( databaseName ), null );
        for ( CatalogSchema schema : schemas ) {
            List<String> tables = new ArrayList<>();
            List<CatalogTable> childTables = catalog.getTables( schema.id, null );
            for ( CatalogTable childTable : childTables ) {
                List<String> table = new ArrayList<>();
                List<CatalogColumn> childColumns = catalog.getColumns( childTable.id );
                for ( CatalogColumn catalogColumn : childColumns ) {
                    table.add( schema.name + "." + childTable.name + "." + catalogColumn.name );
                }
                if ( childTable.tableType == TableType.TABLE ) {
                    tables.addAll( table );
                }
            }
            schemaTree.addAll( tables );
            result.add( schemaTree );
        }
        return result;
    }


    /**
     * Gets all columns in the database
     *
     * @return all the columns
     */
    public List<QueryColumn> getAllColumns() {
        List<QueryColumn> columns = new ArrayList<>();

        List<CatalogColumn> catalogColumns = catalog.getColumns( new Pattern( databaseName ), null, null, null );
        columns.addAll( catalogColumns.stream().map( c -> new QueryColumn( c.getSchemaName(), c.getTableName(), c.name, c.type ) ).collect( Collectors.toList() ) );

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

        List<QueryColumn> columns = new ArrayList<>();

        catalog
                .getColumns( new Pattern( databaseName ), new Pattern( schemaName ), new Pattern( tableName ), null )
                .forEach( c -> columns.add( QueryColumn.fromCatalogColumn( c ) ) );

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
        PolyType type = null;

        try {
            type = catalog.getColumn( databaseName, schema, table, column ).type;
        } catch ( UnknownColumnException | UnknownSchemaException | UnknownDatabaseException | UnknownTableException e ) {
            log.error( "Caught exception", e );
        }
        return type;
    }


    private StatisticResult executeSqlSelect( String query ) {
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        StatisticResult result = new StatisticResult();

        try {
            result = executeSqlSelect( statement, query );
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
            return transactionManager.startTransaction( userName, databaseName, false, "Statistics", MultimediaFlavor.FILE );
        } catch ( UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }
    }

    // -----------------------------------------------------------------------
    //                                Helper
    // -----------------------------------------------------------------------


    private StatisticResult executeSqlSelect( final Statement statement, final String sqlSelect ) throws QueryExecutionException {
        PolyResult signature;
        List<List<Object>> rows;

        try {
            signature = processQuery( statement, sqlSelect );
            rows = signature.getRows( statement, getPageSize() );
        } catch ( Throwable t ) {
            throw new QueryExecutionException( t );
        }

        List<PolyType> types = new ArrayList<>();
        List<String> names = new ArrayList<>();
        int i = 0;
        for ( ColumnMetaData metaData : signature.getColumns() ) {
            types.add( signature.getRowType().getFieldList().get( i ).getType().getPolyType() );
            names.add( metaData.schemaName + "." + metaData.tableName + "." + metaData.columnName );
            i++;
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

    }


    private PolyResult processQuery( Statement statement, String sql ) {
        Processor sqlProcessor = statement.getTransaction().getProcessor( QueryLanguage.SQL );
        Node parsed = sqlProcessor.parse( sql );

        if ( parsed.isA( Kind.DDL ) ) {
            // statistics module should not execute any ddls
            throw new RuntimeException( "No DDL expected here" );
        } else {
            Pair<Node, AlgDataType> validated = sqlProcessor.validate( statement.getTransaction(), parsed, false );
            AlgRoot logicalRoot = sqlProcessor.translate( statement, validated.left, null );

            // Prepare
            return statement.getQueryProcessor().prepareQuery( logicalRoot, true );
        }
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
        return res.getColumns() != null && res.getColumns().length > 0;
    }


    public static String buildQualifiedName( String... strings ) {
        return "\"" + String.join( "\".\"", strings ) + "\"";
    }


    static class QueryExecutionException extends Exception {

        QueryExecutionException( Throwable t ) {
            super( t );
        }

    }

}

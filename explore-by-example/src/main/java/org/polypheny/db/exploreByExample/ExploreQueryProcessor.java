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

package org.polypheny.db.exploreByExample;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.processing.SqlProcessor;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.Transaction.MultimediaFlavor;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.LimitIterator;
import org.polypheny.db.util.Pair;


@Slf4j
public class ExploreQueryProcessor {

    private final TransactionManager transactionManager;
    private final String databaseName;
    private final String userName;


    public ExploreQueryProcessor( final TransactionManager transactionManager, String userName, String databaseName ) {
        this.transactionManager = transactionManager;
        this.userName = userName;
        this.databaseName = databaseName;
    }


    public ExploreQueryProcessor( final TransactionManager transactionManager, Authenticator authenticator ) {
        this( transactionManager, "pa", "APP" );
    }


    private Transaction getTransaction() {
        try {
            return transactionManager.startTransaction( userName, databaseName, false, "Explore-by-Example", MultimediaFlavor.FILE );
        } catch ( UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }
    }


    public List<ExploreQueryResult> getAllUniqueValues( List<String> qualifiedColumnNames, String qualifiedTableName ) {
        String tables = qualifiedTableName.split( "\nFROM " )[1].split( " LIMIT" )[0];
        return qualifiedColumnNames.stream().map( c -> getAllUniqueValuesMethod( c, tables ) ).collect( Collectors.toList() );
    }


    private ExploreQueryResult getAllUniqueValuesMethod( String qualifiedColumn, String qualifiedTableName ) {
        String query = "SELECT " + qualifiedColumn + " FROM " + qualifiedTableName + " GROUP BY " + qualifiedColumn + " LIMIT 200";
        return this.executeSQL( query );
    }


    public ExploreQueryResult executeSQL( String query ) {
        return executeSQL( query, getPageSize() );
    }


    public ExploreQueryResult executeSQL( String query, int pagination ) {
        ExploreQueryResult result = new ExploreQueryResult();
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        try {
            result = executeSqlSelect( statement, query, pagination );
            transaction.commit();
        } catch ( ExploreQueryProcessor.QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while executing a query from the console", e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
        }
        return result;
    }

    // -----------------------------------------------------------------------
    //                                Helper
    // -----------------------------------------------------------------------


    private ExploreQueryResult executeSqlSelect( final Statement statement, final String sqlSelect, final int pagination ) throws ExploreQueryProcessor.QueryExecutionException {
        PolyphenyDbSignature signature;
        List<List<Object>> rows;
        Iterator<Object> iterator = null;
        try {
            signature = processQuery( statement, sqlSelect );
            final Enumerable enumerable = signature.enumerable( statement.getDataContext() );
            //noinspection unchecked
            iterator = enumerable.iterator();
            rows = MetaImpl.collect( signature.cursorFactory, LimitIterator.of( iterator, 200 ), new ArrayList<>() );
        } catch ( Throwable t ) {
            if ( iterator != null ) {
                try {
                    if ( iterator instanceof AutoCloseable ) {
                        ((AutoCloseable) iterator).close();
                    }
                } catch ( Exception e ) {
                    log.error( "Exception while closing result iterator", e );
                }
            }
            throw new ExploreQueryProcessor.QueryExecutionException( t );
        }

        try {
            List<String> typeInfo = new ArrayList<>();
            List<String> name = new ArrayList<>();
            for ( ColumnMetaData metaData : signature.columns ) {
                typeInfo.add( metaData.type.name );
                name.add( metaData.columnName );
            }

            if ( rows.size() == 1 ) {
                for ( List<Object> row : rows ) {
                    if ( row.size() == 1 ) {
                        for ( Object o : row ) {
                            return new ExploreQueryResult( o.toString(), rows.size(), typeInfo, name );
                        }
                    }
                }
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

            return new ExploreQueryResult( d, rows.size(), typeInfo, name );
        } finally {
            try {
                if ( iterator instanceof AutoCloseable ) {
                    ((AutoCloseable) iterator).close();
                }
            } catch ( Exception e ) {
                log.error( "Exception while closing result iterator2", e );
            }
        }
    }


    private PolyphenyDbSignature processQuery( Statement statement, String sql ) {
        PolyphenyDbSignature signature;
        SqlProcessor sqlProcessor = statement.getTransaction().getSqlProcessor();

        SqlNode parsed = sqlProcessor.parse( sql );

        if ( parsed.isA( SqlKind.DDL ) ) {
            // explore by example should not execute any ddls
            throw new RuntimeException( "No DDL expected here" );
        } else {
            Pair<SqlNode, RelDataType> validated = sqlProcessor.validate( statement.getTransaction(), parsed, false );
            RelRoot logicalRoot = sqlProcessor.translate( statement, validated.left );

            // Prepare
            signature = statement.getQueryProcessor().prepareQuery( logicalRoot );
        }
        return signature;
    }


    /**
     * Get the page
     */
    private int getPageSize() {
        return RuntimeConfig.UI_PAGE_SIZE.getInteger();
    }


    static class QueryExecutionException extends Exception {

        QueryExecutionException( Throwable t ) {
            super( t );
        }

    }


}

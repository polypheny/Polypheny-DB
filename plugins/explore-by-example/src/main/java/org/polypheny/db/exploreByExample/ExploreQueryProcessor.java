/*
 * Copyright 2019-2022 The Polypheny Project
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
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.Transaction.MultimediaFlavor;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Pair;


@Slf4j
public class ExploreQueryProcessor {

    private final TransactionManager transactionManager;
    private final long databaseId;
    private final long userId;
    private final static int DEFAULT_SIZE = 200;


    public ExploreQueryProcessor( final TransactionManager transactionManager, long userId, long databaseId ) {
        this.transactionManager = transactionManager;
        this.userId = userId;
        this.databaseId = databaseId;
    }


    public ExploreQueryProcessor( final TransactionManager transactionManager, Authenticator authenticator ) {
        this( transactionManager, Catalog.defaultUserId, Catalog.defaultDatabaseId );
    }


    private Transaction getTransaction() {
        try {
            return transactionManager.startTransaction( userId, databaseId, false, "Explore-by-Example", MultimediaFlavor.FILE );
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
        PolyImplementation result;
        try {
            result = processQuery( statement, sqlSelect );
        } catch ( Throwable t ) {
            throw new ExploreQueryProcessor.QueryExecutionException( t );
        }
        List<List<Object>> rows = result.getRows( statement, DEFAULT_SIZE );

        List<String> typeInfo = new ArrayList<>();
        List<String> name = new ArrayList<>();
        for ( AlgDataTypeField metaData : result.getRowType().getFieldList() ) {
            typeInfo.add( metaData.getType().getFullTypeString() );
            name.add( metaData.getName() );
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

    }


    private PolyImplementation processQuery( Statement statement, String sql ) {
        PolyImplementation result;
        Processor sqlProcessor = statement.getTransaction().getProcessor( QueryLanguage.SQL );

        Node parsed = sqlProcessor.parse( sql ).get( 0 );

        if ( parsed.isA( Kind.DDL ) ) {
            // explore by example should not execute any ddls
            throw new RuntimeException( "No DDL expected here" );
        } else {
            Pair<Node, AlgDataType> validated = sqlProcessor.validate( statement.getTransaction(), parsed, false );
            AlgRoot logicalRoot = sqlProcessor.translate( statement, validated.left, null );

            // Prepare
            result = statement.getQueryProcessor().prepareQuery( logicalRoot, true );
        }
        return result;
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

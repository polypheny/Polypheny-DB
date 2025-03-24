/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.transaction.mvcc;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.ImplementationContext;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.locking.ConcurrencyControlType;
import org.polypheny.db.type.entity.PolyValue;

public class MvccUtils {

    public static boolean isInNamespaceUsingMvcc( Entity entity ) {
        return isNamespaceUsingMvcc( entity.getNamespaceId() );
    }


    public static boolean isInNamespaceUsingMvcc( LogicalRelSnapshot snapshot, String... tableNames ) {
        final List<String> names = ImmutableList.copyOf( tableNames );
        final LogicalTable entity;
        if ( names.size() == 2 ) {
            entity = snapshot.getTable( names.get( 0 ), names.get( 1 ) ).orElseThrow( () -> new GenericRuntimeException( String.join( ".", names ) ) );
        } else if ( names.size() == 1 ) {
            entity = snapshot.getTable( Catalog.DEFAULT_NAMESPACE_NAME, names.get( 0 ) ).orElseThrow( () -> new GenericRuntimeException( String.join( ".", names ) ) );
        } else {
            throw new GenericRuntimeException( "Invalid table name: " + String.join( ".", names ) );
        }
        return isInNamespaceUsingMvcc( entity );
    }


    public static boolean isNamespaceUsingMvcc( long namespaceId ) {
        return isNamespaceUsingMvcc( Catalog.getInstance().getSnapshot().getNamespace( namespaceId ).orElseThrow() );
    }


    public static boolean isNamespaceUsingMvcc( LogicalNamespace namespace ) {
        return namespace.getConcurrencyControlType() == ConcurrencyControlType.MVCC;
    }


    public static ConcurrencyControlType getDefaultConcurrencyControlType( DataModel dataModel ) {
        return switch ( dataModel ) {
            case RELATIONAL -> (ConcurrencyControlType) RuntimeConfig.REL_DEFAULT_CONCURRENCY_CONTROL.getEnum();
            case DOCUMENT -> (ConcurrencyControlType) RuntimeConfig.DOC_DEFAULT_CONCURRENCY_CONTROL.getEnum();
            case GRAPH -> (ConcurrencyControlType) RuntimeConfig.GRAPH_DEFAULT_CONCURRENCY_CONTROL.getEnum();
        };
    }


    public static ExecutedContext executeStatement( QueryLanguage language, String query, long namespaceId, Transaction transaction ) {
        return executeStatement( language, query, namespaceId, null, transaction );
    }


    public static ExecutedContext executeStatement( QueryLanguage language, String query, long namespaceId, Statement statement ) {
        return executeStatement( language, query, namespaceId, statement, statement.getTransaction() );
    }


    public static ExecutedContext executeStatement( QueryLanguage language, String query, long namespaceId, Statement statement, Transaction transaction ) {
        ImplementationContext context = LanguageManager.getINSTANCE().anyPrepareQuery(
                QueryContext.builder()
                        .query( query )
                        .statement( statement )
                        .language( language )
                        .origin( transaction.getOrigin() )
                        .namespaceId( namespaceId )
                        .transactionManager( transaction.getTransactionManager() )
                        .isMvccInternal( true )
                        .build(), transaction ).get( 0 );

        if ( context.getException().isPresent() ) {
            throw new RuntimeException( context.getException().get() );
        }

        return context.execute( context.getStatement() );
    }


    public static List<ExecutedContext> executeDmlAlgTree( AlgRoot root, Statement statement, long namespaceId ) {
        PolyImplementation implementation = statement.getQueryProcessor().prepareQuery( root, true );

        ParsedQueryContext dummyContext = getDummyContext( statement, namespaceId );
        ImplementationContext implementationContext = new ImplementationContext( implementation, dummyContext, statement, null );
        List<ExecutedContext> executedContexts = new ArrayList<>();
        try {
            if ( implementationContext.getException().isPresent() ) {
                throw implementationContext.getException().get();
            }
            executedContexts.add( implementationContext.execute( implementationContext.getStatement() ) );
        } catch ( Throwable e ) {
            Transaction transaction = implementationContext.getStatement().getTransaction();
            if ( transaction.isAnalyze() && implementationContext.getException().isEmpty() ) {
                transaction.getQueryAnalyzer().attachStacktrace( e );
            }
            if ( transaction.isActive() ) {
                transaction.rollback( e.getMessage() );
            }
            executedContexts.add( ExecutedContext.ofError( e, implementationContext, null ) );
            return executedContexts;
        }
        return executedContexts;
    }


    private static ParsedQueryContext getDummyContext( Statement statement, long namespaceId ) {
        QueryContext dummyContext = QueryContext.builder()
                .query( "" )
                .statement( statement )
                .language( QueryLanguage.from( "sql" ) )
                .origin( statement.getTransaction().getOrigin() )
                .namespaceId( namespaceId )
                .transactionManager( statement.getTransaction().getTransactionManager() )
                .build();
        return ParsedQueryContext.fromQuery( "", null, dummyContext );
    }


    public static long collectLong( PolyValue value ) {
        return switch ( value.getType() ) {
            case BIGINT -> value.asLong().getValue();
            case DECIMAL -> value.asBigDecimal().longValue();
            default -> throw new IllegalArgumentException( "Unsupported commit validation result of type: " + value.getType() );
        };
    }

}



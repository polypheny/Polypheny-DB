/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.protointerface.statements;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.protointerface.PIClient;
import org.polypheny.db.protointerface.PIServiceException;
import org.polypheny.db.protointerface.proto.ExecuteUnparameterizedStatementRequest;
import org.polypheny.db.protointerface.proto.PrepareStatementRequest;

@Slf4j
public class StatementManager {

    private final AtomicInteger statementIdGenerator;
    private final PIClient client;
    private ConcurrentHashMap<Integer, PIStatement> openStatements;
    private ConcurrentHashMap<Integer, PIUnparameterizedStatementBatch> openUnparameterizedBatches;


    public StatementManager( PIClient client ) {
        this.client = client;
        statementIdGenerator = new AtomicInteger( 1 );
        openStatements = new ConcurrentHashMap<>();
        openUnparameterizedBatches = new ConcurrentHashMap<>();
    }


    private LogicalNamespace getNamespace( String namespaceName ) {
        Optional<LogicalNamespace> optionalNamespace = Catalog.getInstance().getSnapshot().getNamespace( namespaceName );

        if ( optionalNamespace.isEmpty() ) {
            throw new PIServiceException( "Getting namespace " + namespaceName + " failed." );
        }
        return optionalNamespace.get();
    }


    public PIUnparameterizedStatement createUnparameterizedStatement( ExecuteUnparameterizedStatementRequest request ) throws PIServiceException {
        synchronized ( client ) {
            String languageName = request.getLanguageName();
            if ( !isSupportedLanguage( languageName ) ) {
                throw new PIServiceException( "Language " + languageName + " not supported." );
            }
            int statementId = statementIdGenerator.getAndIncrement();
            LogicalNamespace namespace = client.getNamespace();
            if ( request.hasNamespaceName() ) {
                namespace = getNamespace( request.getNamespaceName() );
            }
            assert namespace != null;
            PIUnparameterizedStatement statement = new PIUnparameterizedStatement(
                    statementId,
                    client,
                    QueryLanguage.from( languageName ),
                    namespace,
                    request.getStatement()
            );
            openStatements.put( statementId, statement );
            if ( log.isTraceEnabled() ) {
                log.trace( "created request {}", statement );
            }
            return statement;
        }
    }


    public PIUnparameterizedStatementBatch createUnparameterizedStatementBatch( List<ExecuteUnparameterizedStatementRequest> statements ) {
        synchronized ( client ) {
            List<PIUnparameterizedStatement> PIUnparameterizedStatements = statements.stream()
                    .map( this::createUnparameterizedStatement )
                    .collect( Collectors.toList() );
            final int batchId = statementIdGenerator.getAndIncrement();
            final PIUnparameterizedStatementBatch batch = new PIUnparameterizedStatementBatch( batchId, client, PIUnparameterizedStatements );
            openUnparameterizedBatches.put( batchId, batch );
            if ( log.isTraceEnabled() ) {
                log.trace( "created batch {}", batch );
            }
            return batch;
        }
    }


    public PIPreparedIndexedStatement createIndexedPreparedInterfaceStatement( PrepareStatementRequest request ) throws PIServiceException {
        synchronized ( client ) {
            String languageName = request.getLanguageName();
            if ( !isSupportedLanguage( languageName ) ) {
                throw new PIServiceException( "Language " + languageName + " not supported." );
            }
            int statementId = statementIdGenerator.getAndIncrement();
            LogicalNamespace namespace = client.getNamespace();
            if ( request.hasNamespaceName() ) {
                namespace = getNamespace( request.getNamespaceName() );
            }
            assert namespace != null;
            PIPreparedIndexedStatement statement = new PIPreparedIndexedStatement(
                    statementId,
                    client,
                    QueryLanguage.from( languageName ),
                    namespace,
                    request.getStatement()
            );
            openStatements.put( statementId, statement );
            if ( log.isTraceEnabled() ) {
                log.trace( "created named prepared statement {}", statement );
            }
            return statement;
        }
    }


    public PIPreparedNamedStatement createNamedPreparedInterfaceStatement( PrepareStatementRequest request ) throws PIServiceException {
        synchronized ( client ) {
            String languageName = request.getLanguageName();
            if ( !isSupportedLanguage( languageName ) ) {
                throw new PIServiceException( "Language " + languageName + " not supported." );
            }
            final int statementId = statementIdGenerator.getAndIncrement();
            LogicalNamespace namespace = client.getNamespace();
            if ( request.hasNamespaceName() ) {
                namespace = getNamespace( request.getNamespaceName() );
            }
            assert namespace != null;
            PIPreparedNamedStatement statement = new PIPreparedNamedStatement(
                    statementId,
                    client,
                    QueryLanguage.from( languageName ),
                    namespace,
                    request.getStatement()
            );
            openStatements.put( statementId, statement );
            if ( log.isTraceEnabled() ) {
                log.trace( "created named prepared statement {}", statement );
            }
            return statement;
        }
    }


    public void closeAll() {
        synchronized ( client ) {
            openUnparameterizedBatches.values().forEach( this::closeBatch );
            openStatements.values().forEach( s -> closeStatement( s.getId() ) );
        }
    }


    public void closeBatch( PIUnparameterizedStatementBatch toClose ) {
        synchronized ( client ) {
            toClose.getStatements().forEach( s -> closeStatementOrBatch( s.getId() ) );
        }
    }


    private void closeStatement( int statementId ) {
        synchronized ( client ) {
            PIStatement statementToClose = openStatements.remove( statementId );
            if ( statementToClose == null ) {
                return;
            }
            statementToClose.closeResults();
        }
    }


    public void closeStatementOrBatch( int statementId ) {
        synchronized ( client ) {
            PIUnparameterizedStatementBatch batchToClose = openUnparameterizedBatches.remove( statementId );
            if ( batchToClose != null ) {
                closeBatch( batchToClose );
                return;
            }
            closeStatement( statementId );
        }
    }


    public PIStatement getStatement( int statementId ) {
        if ( statementId == 0 ) {
            throw new PIServiceException( "Invalid statement id: 0 (possibly uninitialized protobuf field)" );
        }
        PIStatement statement = openStatements.get( statementId );
        if ( statement == null ) {
            throw new PIServiceException( "A statement with id " + statementId + " does not exist for that client" );
        }
        return statement;
    }


    public PIPreparedNamedStatement getNamedPreparedStatement( int statementId ) throws PIServiceException {
        PIStatement statement = getStatement( statementId );
        if ( !(statement instanceof PIPreparedNamedStatement) ) {
            throw new PIServiceException( "A named prepared statement with id " + statementId + " does not exist for that client" );
        }
        return (PIPreparedNamedStatement) statement;
    }


    public PIPreparedIndexedStatement getIndexedPreparedStatement( int statementId ) throws PIServiceException {
        PIStatement statement = getStatement( statementId );
        if ( !(statement instanceof PIPreparedIndexedStatement) ) {
            throw new PIServiceException( "A prepared indexed statement with id " + statementId + " does not exist for that client" );
        }
        return (PIPreparedIndexedStatement) statement;
    }


    public boolean isSupportedLanguage( String statementLanguageName ) {
        return LanguageManager.getLanguages()
                .stream()
                .map( QueryLanguage::getSerializedName )
                .collect( Collectors.toSet() )
                .contains( statementLanguageName );
    }

}

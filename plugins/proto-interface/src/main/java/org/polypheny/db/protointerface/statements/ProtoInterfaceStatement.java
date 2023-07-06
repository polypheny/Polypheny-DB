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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.protointerface.ProtoInterfaceClient;
import org.polypheny.db.protointerface.ProtoInterfaceServiceException;
import org.polypheny.db.protointerface.proto.ColumnMeta;
import org.polypheny.db.protointerface.proto.Frame;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.db.protointerface.relational.RelationalMetaRetriever;
import org.polypheny.db.protointerface.utils.PropertyUtils;
import org.polypheny.db.protointerface.utils.ProtoUtils;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.LimitIterator;
import org.polypheny.db.util.Pair;

@Slf4j
public abstract class ProtoInterfaceStatement {


    @Getter
    protected final int statementId;
    protected final ProtoInterfaceClient protoInterfaceClient;
    protected final StopWatch executionStopWatch;
    protected final QueryLanguage queryLanguage;
    protected final String query;
    protected PolyImplementation<Object> currentImplementation;
    protected Iterator<Object> resultIterator;



    public ProtoInterfaceStatement( int statementId, ProtoInterfaceClient protoInterfaceClient, QueryLanguage queryLanguage, String query ) {
        if ( query == null ) {
            throw new NullPointerException( "statement must not be null." );
        }
        if ( protoInterfaceClient == null ) {
            throw new NullPointerException( "proto interface client must not be null." );
        }
        if ( queryLanguage == null ) {
            throw new NullPointerException( "query language must not be null." );
        }
        this.statementId = statementId;
        this.protoInterfaceClient = protoInterfaceClient;
        this.queryLanguage = queryLanguage;
        this.query = query;
        this.executionStopWatch = new StopWatch();
    }


    public abstract StatementResult execute() throws Exception;


    protected StatementResult execute( Statement statement ) throws Exception {
        Processor queryProcessor = statement.getTransaction().getProcessor( queryLanguage );
        Node parsedStatement = queryProcessor.parse( query ).get( 0 );
        if ( parsedStatement.isA( Kind.DDL ) ) {
            currentImplementation = queryProcessor.prepareDdl( statement, parsedStatement,
                    new QueryParameters( query, queryLanguage.getNamespaceType() ) );
        } else {
            Pair<Node, AlgDataType> validated = queryProcessor.validate( protoInterfaceClient.getCurrentTransaction(),
                    parsedStatement, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean() );
            AlgRoot logicalRoot = queryProcessor.translate( statement, validated.left, null );
            AlgDataType parameterRowType = queryProcessor.getParameterRowType( validated.left );
            currentImplementation = statement.getQueryProcessor().prepareQuery( logicalRoot, parameterRowType, true );
        }

        StatementResult.Builder resultBuilder = StatementResult.newBuilder();
        if ( Kind.DDL.contains( currentImplementation.getKind() ) ) {
            resultBuilder.setScalar( 1 );
            commitIfAuto();
            return resultBuilder.build();
        }
        if ( Kind.DML.contains( currentImplementation.getKind() ) ) {
            resultBuilder.setScalar( currentImplementation.getRowsChanged( statement ) );
            commitIfAuto();
            return resultBuilder.build();
        }
        //resultBuilder.setScalar( currentImplementation.getRowsChanged( statement ) );
        commitIfAuto();
        resultBuilder.setFrame( fetchFirst() );
        return resultBuilder.build();
    }


    protected void commitIfAuto() throws IllegalArgumentException {
        if ( !protoInterfaceClient.isAutocommit() ) {
            return;
        }
        protoInterfaceClient.commitCurrentTransaction();
    }


    public Frame fetchFirst() throws Exception {
        return fetch( 0 );
    }


    public Frame fetch( long offset ) throws Exception {
        int fetchSize = Integer.parseInt( PropertyUtils.getDefaultOf( PropertyUtils.FETCH_SIZE_KEY ) );
        return fetch( offset, fetchSize );
    }


    public Frame fetch( long offset, int fetchSize ) throws Exception {
        switch ( queryLanguage.getNamespaceType() ) {
            case RELATIONAL:
                return relationalFetch( offset, fetchSize );
            case GRAPH:
                return graphFetch( offset, fetchSize );
            case DOCUMENT:
                return documentFetch( offset, fetchSize );
        }
        throw new ProtoInterfaceServiceException( "Should never be thrown." );
    }


    public Frame relationalFetch( long offset, int fetchSize ) throws Exception {
        if ( currentImplementation == null ) {
            throw new ProtoInterfaceServiceException( "Can't fetch frames of an unexecuted statement" );
        }
        synchronized ( protoInterfaceClient ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "fetch(long {}, int {} )", offset, fetchSize );
            }
            Iterator<Object> iterator = getOrCreateIterator();
            CursorFactory cursorFactory = currentImplementation.getCursorFactory();
            Iterator<Object> sectionIterator = LimitIterator.of( iterator, fetchSize );
            startOrResumeStopwatch();
            // TODO TH: clean up this mess
            List<List<PolyValue>> rows = (List<List<PolyValue>>) (List<?>) MetaImpl.collect( cursorFactory, sectionIterator, new ArrayList<>() );
            executionStopWatch.suspend();
            boolean isDone = fetchSize == 0 || rows.size() < fetchSize;
            if ( isDone ) {
                executionStopWatch.stop();
                currentImplementation.getExecutionTimeMonitor().setExecutionTime( executionStopWatch.getNanoTime() );
            }
            List<ColumnMeta> columnMetas = RelationalMetaRetriever.retrieveColumnMetas( currentImplementation );
            return ProtoUtils.buildRelationalFrame( offset, isDone, rows, columnMetas );
        }
    }


    private Frame graphFetch( long offset, int fetchSize ) {
        throw new NotImplementedException( "Graph Fetching is not yet implmented." );
    }


    private Frame documentFetch( long offset, int fetchSize ) {
        throw new NotImplementedException( "Doument fetching is no yet implemented." );
    }


    protected Iterator<Object> getOrCreateIterator() {
        if ( resultIterator != null ) {
            return resultIterator;
        }
        Statement statement = currentImplementation.getStatement();
        final Enumerable<Object> enumerable = currentImplementation.getBindable().bind( statement.getDataContext() );
        resultIterator = enumerable.iterator();
        return resultIterator;
    }


    protected void startOrResumeStopwatch() {
        if ( executionStopWatch.isSuspended() ) {
            executionStopWatch.resume();
            return;
        }
        if ( executionStopWatch.isStopped() ) {
            executionStopWatch.start();
        }
    }


    protected void commitElseRollback() {
        try {
            protoInterfaceClient.commitCurrentTransaction();
        } catch ( Exception e ) {
            protoInterfaceClient.rollbackCurrentTransaction();
        }
    }

}

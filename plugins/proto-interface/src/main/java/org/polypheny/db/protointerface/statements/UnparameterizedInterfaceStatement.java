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
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.logistic.NamespaceType;
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
import org.polypheny.db.protointerface.relational.RelationalUtils;
import org.polypheny.db.protointerface.utils.ProtoUtils;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.LimitIterator;
import org.polypheny.db.util.Pair;

@Slf4j
public class UnparameterizedInterfaceStatement extends ProtoInterfaceStatement {

    public UnparameterizedInterfaceStatement( int statementId, ProtoInterfaceClient protoInterfaceClient, QueryLanguage queryLanguage, String query ) {
        super( statementId, protoInterfaceClient, queryLanguage, query );
    }


    public StatementResult execute() throws Exception {
        Statement currentStatement = protoInterfaceClient.getCurrentOrCreateNewTransaction().createStatement();
        Processor queryProcessor = currentStatement.getTransaction().getProcessor( queryLanguage );
        Node parsedStatement = queryProcessor.parse( query ).get( 0 );
        if ( parsedStatement.isA( Kind.DDL ) ) {
            currentImplementation = queryProcessor.prepareDdl( currentStatement, parsedStatement,
                    new QueryParameters( query, queryLanguage.getNamespaceType() ) );
        } else {
            Pair<Node, AlgDataType> validated = queryProcessor.validate( protoInterfaceClient.getCurrentTransaction(),
                    parsedStatement, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean() );
            AlgRoot logicalRoot = queryProcessor.translate( currentStatement, validated.left, null );
            AlgDataType parameterRowType = queryProcessor.getParameterRowType( validated.left );
            currentImplementation = currentStatement.getQueryProcessor().prepareQuery( logicalRoot, parameterRowType, true );
        }

        StatementResult.Builder resultBuilder = StatementResult.newBuilder();
        if ( Kind.DDL.contains( currentImplementation.getKind() ) ) {
            resultBuilder.setScalar( 1 );
            commitElseRollback();
            return resultBuilder.build();
        }
        if ( Kind.DML.contains( currentImplementation.getKind() ) ) {
            resultBuilder.setScalar( currentImplementation.getRowsChanged( currentStatement ) );
            commitElseRollback();
            return resultBuilder.build();
        }
        // TODO TH: replace hardcoded value with cont from the request
        resultBuilder.setFrame( fetchFirst() );
        return resultBuilder.build();
    }


    @Override
    public Frame fetch( long offset ) {
        switch ( queryLanguage.getNamespaceType() ) {
            case RELATIONAL:
                return relationalFetch( offset );
            case GRAPH:
                return graphFetch( offset );
            case DOCUMENT:
                return documentFetch( offset );
        }
        throw new ProtoInterfaceServiceException( "Should never be thrown." );
    }


    private Frame documentFetch( long offset ) {
        throw new NotImplementedException( "Doument fetching is no yet implemented." );
    }


    private Frame graphFetch( long offset ) {
        throw new NotImplementedException( "Graph Fetching is not yet implmented." );
    }


    public Frame relationalFetch( long offset ) {
        if (currentImplementation == null) {
            throw new ProtoInterfaceServiceException( "Can't fetch frames of an unexecuted statement" );
        }
        synchronized ( protoInterfaceClient ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "fetch(long {}, int {} )", offset, maxRowCount );
            }
            Iterator<Object> iterator = getOrCreateIterator();
            Meta.CursorFactory cursorFactory = currentImplementation.getCursorFactory();
            Iterator<Object> sectionIterator = LimitIterator.of( iterator, maxRowCount );
            startOrResumeStopwatch();
            // TODO TH: clean up this mess
            List<List<PolyValue>> rows = (List<List<PolyValue>>)(List<?>)MetaImpl.collect( cursorFactory, sectionIterator, new ArrayList<>() );
            //List<String> column_labels = TODO get row names
            executionStopWatch.suspend();
            boolean isDone = maxRowCount == 0 || rows.size() < maxRowCount;
            if ( isDone ) {
                executionStopWatch.stop();
                currentImplementation.getExecutionTimeMonitor().setExecutionTime( executionStopWatch.getNanoTime() );
            }
            List<ColumnMeta> columnMetas = RelationalMetaRetriever.retrieveColumnMetas( currentImplementation );
            return RelationalUtils.buildRelationalFrame( offset, isDone, rows, columnMetas );
        }
    }

}

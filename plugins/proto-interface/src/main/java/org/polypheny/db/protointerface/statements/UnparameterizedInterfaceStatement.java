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
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.protointerface.PropertyKeys;
import org.polypheny.db.protointerface.ProtoInterfaceClient;
import org.polypheny.db.protointerface.ProtoInterfaceServiceException;
import org.polypheny.db.protointerface.proto.ColumnMeta;
import org.polypheny.db.protointerface.proto.Frame;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.db.protointerface.relational.RelationalMetaRetriever;
import org.polypheny.db.protointerface.relational.RelationalUtils;
import org.polypheny.db.protointerface.relational.RelationalMetaRetriever;
import org.polypheny.db.protointerface.relational.RelationalUtils;
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
            commitIfAuto();
            return resultBuilder.build();
        }
        if ( Kind.DML.contains( currentImplementation.getKind() ) ) {
            resultBuilder.setScalar( currentImplementation.getRowsChanged( currentStatement ) );
            commitIfAuto();
            return resultBuilder.build();
        }
        commitIfAuto();
        resultBuilder.setFrame( fetchFirst() );
        return resultBuilder.build();
    }


    @Override
    public Frame fetch( long offset, int fetchSize) {
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

    @Override
    public Frame fetch( long offset ) {
        int fetchSize = Integer.parseInt( PropertyKeys.getDefaultOf( PropertyKeys.FETCH_SIZE ) );
        return fetch( offset,  fetchSize);
    }

    private Frame documentFetch( long offset, int fetchSize ) {
        throw new NotImplementedException( "Doument fetching is no yet implemented." );
    }


    private Frame graphFetch( long offset, int fetchSize ) {
        throw new NotImplementedException( "Graph Fetching is not yet implmented." );
    }


    public Frame relationalFetch( long offset, int fetchSize ) {
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
            //List<String> column_labels = TODO get row names
            executionStopWatch.suspend();
            boolean isDone = fetchSize == 0 || rows.size() < fetchSize;
            if ( isDone ) {
                executionStopWatch.stop();
                currentImplementation.getExecutionTimeMonitor().setExecutionTime( executionStopWatch.getNanoTime() );
            }
            List<ColumnMeta> columnMetas = RelationalMetaRetriever.retrieveColumnMetas( currentImplementation  );
            return RelationalUtils.buildRelationalFrame( offset, isDone, rows , columnMetas );
        }
    }

}

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

import java.util.Iterator;
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
import org.polypheny.db.protointerface.proto.Frame;
import org.polypheny.db.protointerface.proto.QueryResult;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.Pair;

@Slf4j
public class UnparameterizedStatement extends ProtoInterfaceStatement {
    protected PolyImplementation currentImplementation;
    protected Statement currentStatement;

    public UnparameterizedStatement( int statementId, ProtoInterfaceClient protoInterfaceClient, QueryLanguage queryLanguage, String query ) {
        super( statementId, protoInterfaceClient, queryLanguage, query );
    }


    public QueryResult execute() throws Exception {
        currentStatement = protoInterfaceClient.getCurrentOrCreateNewTransaction().createStatement();
        Processor queryProcessor = currentStatement.getTransaction().getProcessor( queryLanguage );
        Node parsedStatement = queryProcessor.parse( query ).get( 0 );
        if ( parsedStatement.isA( Kind.DDL ) ) {
            // TODO TH: namespace type according to language
            currentImplementation = queryProcessor.prepareDdl( currentStatement, parsedStatement, new QueryParameters( query, NamespaceType.RELATIONAL ) );

        } else {
            Pair<Node, AlgDataType> validated = queryProcessor.validate( protoInterfaceClient.getCurrentTransaction(),
                    parsedStatement, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean() );
            AlgRoot logicalRoot = queryProcessor.translate( currentStatement, validated.left, null );
            AlgDataType parameterRowType = queryProcessor.getParameterRowType( validated.left );
            currentImplementation = currentStatement.getQueryProcessor().prepareQuery( logicalRoot, parameterRowType, true );
        }

        QueryResult.Builder resultBuilder = QueryResult.newBuilder();
        if ( Kind.DDL.contains( currentImplementation.getKind() ) ) {
            resultBuilder.setRowCount( 1 );
            commitElseRollback();
            return resultBuilder.build();
        }
        if ( Kind.DML.contains( currentImplementation.getKind() ) ) {
            resultBuilder.setRowCount( currentImplementation.getRowsChanged( currentStatement ) );
            commitElseRollback();
            return resultBuilder.build();
        }
        // Fetch data and return
        return resultBuilder.build();
    }

    public Frame fetch(long offset, final int fetchMaxRowCount ) {
        synchronized ( protoInterfaceClient ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "fetch( statement {}, long {}, int {} )", currentStatement, offset, fetchMaxRowCount );
            }
            startOrResumeStopwatch();
            List<List<Object>> rows = currentImplementation.getRows( currentStatement, fetchMaxRowCount );
            executionStopWatch.suspend();
            boolean isDone = fetchMaxRowCount == 0 || rows.size() < fetchMaxRowCount;
            if (isDone) {
                executionStopWatch.stop();
                currentImplementation.getExecutionTimeMonitor().setExecutionTime( executionStopWatch.getNanoTime() );
            }
            //TODO TH: handle values
        }
        return Frame.newBuilder().build();
    }
}
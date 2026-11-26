/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.prisminterface.statements;

import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prisminterface.NamedValueProcessor;
import org.polypheny.db.prisminterface.PIClient;
import org.polypheny.db.prisminterface.statementProcessing.StatementProcessor;
import org.polypheny.db.processing.Processor.PreparedNamedQuery;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.prism.ParameterMeta;
import org.polypheny.prism.StatementResult;

public class PIPreparedNamedStatement extends PIPreparedStatement {

    @Getter
    private final String query;
    private final NamedValueProcessor namedValueProcessor;
    private final PreparedNamedQuery graphQuery;
    @Getter
    @Setter
    private PolyImplementation implementation;
    @Getter
    @Setter
    private Statement statement;


    public PIPreparedNamedStatement(
            int id,
            PIClient client,
            QueryLanguage language,
            LogicalNamespace namespace,
            String query ) {
        super(
                id, client, language, namespace
        );
        if ( language.dataModel() == DataModel.GRAPH ) {
            this.query = query;
            this.namedValueProcessor = null;
            this.graphQuery = language.processorSupplier().get().prepareNamedQuery( query, namespace );
            this.parameterMetas = graphQuery.parameterNames().stream()
                    .map( name -> ParameterMeta.newBuilder()
                            .setName( name )
                            .setParameterName( name )
                            .setTypeName( PolyType.ANY.getTypeName() )
                            .build() )
                    .toList();
        } else {
            this.namedValueProcessor = new NamedValueProcessor( query );
            this.query = namedValueProcessor.getProcessedQuery();
            this.graphQuery = null;
        }
    }


    @SuppressWarnings("Duplicates")
    public StatementResult execute( Map<String, PolyValue> values, int fetchSize ) throws Exception {
        if ( graphQuery != null ) {
            return executeGraphQuery( values, fetchSize );
        }
        List<ParsedQueryContext> preParsedQueries = StatementProcessor.parseWithOpenAutocommitResult( this );
        List<PolyValue> valueList = namedValueProcessor.transformValueMap( values );
        return executeInTransaction( preParsedQueries != null, () -> {
            for ( int i = 0; i < valueList.size(); i++ ) {
                AlgDataType type = (parameterPolyTypes != null && i < parameterPolyTypes.size())
                        ? deriveType( statement.getDataContext().getTypeFactory(), parameterPolyTypes.get( i ) )
                        : PolyValue.deriveType( valueList.get( i ), statement.getDataContext().getTypeFactory() );
                statement.getDataContext().addParameterValues( i, type, List.of( valueList.get( i ) ) );
            }
            StatementProcessor.implement( this, null, preParsedQueries );
            return StatementProcessor.executeAndGetResult( this, fetchSize );
        } );
    }


    private StatementResult executeGraphQuery( Map<String, PolyValue> values, int fetchSize ) {
        Node parsed = graphQuery.bind().apply( values );
        close();
        statement = client.getOrCreateNewTransaction().createStatement();
        implementation = null;
        StatementProcessor.implement( this, parsed );
        return StatementProcessor.executeAndGetResult( this, fetchSize );
    }


    @Override
    public void close() {
        closeResults();
        if ( statement != null ) {
            statement.close();
        }
    }


    @Override
    public Transaction getTransaction() {
        return statement != null ? statement.getTransaction() : null;
    }

}

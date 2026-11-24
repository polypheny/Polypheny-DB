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
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.prisminterface.PIClient;
import org.polypheny.db.prisminterface.PIServiceException;
import org.polypheny.db.prisminterface.statementProcessing.StatementProcessor;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.prism.BatchGeneratedKeys;
import org.polypheny.prism.ParameterMeta;
import org.polypheny.prism.StatementBatchResponse;
import org.polypheny.prism.StatementResult;

@Getter
public class PIPreparedIndexedStatement extends PIPreparedStatement {

    private final String query;
    @Setter
    private Statement statement;
    @Setter
    private PolyImplementation implementation;
    @Setter
    private boolean sequentialBatchExecution;


    public PIPreparedIndexedStatement(
            int id,
            PIClient client,
            QueryLanguage language,
            LogicalNamespace namespace,
            String query ) {
        super(
                id, client, language, namespace
        );
        this.query = query;
    }


    public List<Long> executeBatch( List<List<PolyValue>> valuesBatch ) {
        List<ParsedQueryContext> preParsedQueries = StatementProcessor.parseWithOpenAutocommitResult( this );
        return executeInTransaction( preParsedQueries != null, () -> {
            List<AlgDataType> types = IntStream.range( 0, valuesBatch.size() )
                    .mapToObj( i -> deriveType( statement.getTransaction().getTypeFactory(), parameterPolyTypes.get( i ) ) )
                    .toList();
            int index = 0;
            for ( List<PolyValue> column : valuesBatch ) {
                statement.getDataContext().addParameterValues( index, types.get( index ), column );
                index++;
            }
            StatementProcessor.implement( this, null, preParsedQueries );
            return List.of( StatementProcessor.executeAndGetResult( this ).getScalar() );
        } );
    }


    public StatementBatchResponse executeSequentialBatch( List<List<PolyValue>> bindings ) {
        List<ParameterMeta> metadata = getParameterMetas();
        if ( bindings.stream().anyMatch( row -> row.size() != metadata.size() ) ) {
            throw new PIServiceException( "Incorrect number of parameters in prepared batch." );
        }
        if ( StatementProcessor.parseUpdateBatchMember( this ).isDdl() ) {
            throw new PIServiceException( "DDL must execute separately from the prepared batch." );
        }
        StatementProcessor.parseWithOpenAutocommitResult( this );
        return client.isAutoCommit()
                ? client.executeAtomicBatch( () -> executeBindings( bindings, metadata ) )
                : executeBindings( bindings, metadata );
    }


    private StatementBatchResponse executeBindings( List<List<PolyValue>> bindings, List<ParameterMeta> metadata ) {
        StatementBatchResponse.Builder response = StatementBatchResponse.newBuilder().setBatchId( id );
        for ( int i = 0; i < bindings.size(); i++ ) {
            StatementResult result = execute( bindings.get( i ), metadata, -1 );
            response.addScalars( result.getScalar() );
            if ( result.hasGeneratedKeys() ) {
                response.addGeneratedKeys( BatchGeneratedKeys.newBuilder().setStatementIndex( i ).setKeys( result.getGeneratedKeys() ) );
            }
        }
        return response.build();
    }


    @SuppressWarnings("Duplicates")
    public StatementResult execute( List<PolyValue> values, List<ParameterMeta> parameterMetas, int fetchSize ) {
        List<ParsedQueryContext> preParsedQueries = StatementProcessor.parseWithOpenAutocommitResult( this );
        return executeInTransaction( preParsedQueries != null, () -> {
            int index = 0;
            for ( PolyValue value : values ) {
                if ( value != null ) {
                    AlgDataType algDataType = parameterMetas.size() > index
                            ? deriveType( statement.getTransaction().getTypeFactory(), parameterPolyTypes.get( index ) )
                            : statement.getTransaction().getTypeFactory().createPolyType( value.type );
                    statement.getDataContext().addParameterValues( index++, algDataType, List.of( value ) );
                }
            }
            StatementProcessor.implement( this, null, preParsedQueries );
            return StatementProcessor.executeAndGetResult( this, fetchSize );
        } );
    }


    @Override
    public void close() {
        if ( statement != null ) {
            statement.close();
        }
        closeResults();
    }


    @Override
    public Transaction getTransaction() {
        return statement != null ? statement.getTransaction() : null;
    }

}

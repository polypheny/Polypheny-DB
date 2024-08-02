/*
 * Copyright 2019-2024 The Polypheny Project
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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.prisminterface.PIClient;
import org.polypheny.db.prisminterface.statementProcessing.StatementProcessor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.prism.ParameterMeta;
import org.polypheny.prism.StatementResult;

@Getter
public class PIPreparedIndexedStatement extends PIPreparedStatement {

    protected String query;
    protected Statement statement;
    @Setter
    protected PolyImplementation implementation;


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
        List<Long> updateCounts = new ArrayList<>();
        if ( statement == null || client.hasNoTransaction() ) {
            statement = client.getOrCreateNewTransaction().createStatement();
        } else {
            statement.getDataContext().resetParameterValues();
        }
        List<AlgDataType> types = IntStream.range( 0, valuesBatch.size() ).mapToObj( i -> deriveType( statement.getTransaction().getTypeFactory(), parameterMetas.get( i ) ) ).toList();
        int i = 0;
        for ( List<PolyValue> column : valuesBatch ) {
            statement.getDataContext().addParameterValues( i, types.get( i++ ), column );
        }
        StatementProcessor.implement( this );
        updateCounts.add( StatementProcessor.executeAndGetResult( this ).getScalar() );
        return updateCounts;
    }


    @SuppressWarnings("Duplicates")
    public StatementResult execute( List<PolyValue> values, List<ParameterMeta> parameterMetas, int fetchSize ) {
        if ( statement == null || client.hasNoTransaction() ) {
            statement = client.getOrCreateNewTransaction().createStatement();
        } else {
            statement.getDataContext().resetParameterValues();
        }
        long index = 0;
        for ( PolyValue value : values ) {
            if ( value != null ) {
                AlgDataType algDataType = parameterMetas.size() > index
                        ? deriveType( statement.getTransaction().getTypeFactory(), parameterMetas.get( (int) index ) )
                        : statement.getTransaction().getTypeFactory().createPolyType( value.type );
                statement.getDataContext().addParameterValues( index++, algDataType, List.of( value ) );
            }
        }
        StatementProcessor.implement( this );
        return StatementProcessor.executeAndGetResult( this, fetchSize );
    }


    private AlgDataType deriveType( JavaTypeFactory typeFactory, ParameterMeta parameterMeta ) {
        return switch ( parameterMeta.getTypeName().toUpperCase() ) {
            case "DECIMAL" -> {
                if ( parameterMeta.getPrecision() >= 0 && parameterMeta.getScale() >= 0 ) {
                    yield typeFactory.createPolyType( PolyType.DECIMAL, parameterMeta.getPrecision(), parameterMeta.getScale() );
                } else if ( parameterMeta.getPrecision() >= 0 ) {
                    yield typeFactory.createPolyType( PolyType.DECIMAL, parameterMeta.getPrecision() );
                }
                yield typeFactory.createPolyType( PolyType.DECIMAL );
            }
            case "DOUBLE" -> typeFactory.createPolyType( PolyType.DOUBLE );
            case "FLOAT" -> typeFactory.createPolyType( PolyType.FLOAT );
            case "INT", "INTEGER" -> typeFactory.createPolyType( PolyType.INTEGER );
            case "VARCHAR" -> {
                if ( parameterMeta.getPrecision() > 0 ) {
                    yield typeFactory.createPolyType( PolyType.VARCHAR, parameterMeta.getPrecision() );
                }
                yield typeFactory.createPolyType( PolyType.VARCHAR );
            }
            case "CHAR" -> {
                if ( parameterMeta.getPrecision() > 0 ) {
                    yield typeFactory.createPolyType( PolyType.CHAR, parameterMeta.getPrecision() );
                }
                yield typeFactory.createPolyType( PolyType.CHAR );
            }
            case "TEXT" -> typeFactory.createPolyType( PolyType.TEXT );
            case "JSON" -> typeFactory.createPolyType( PolyType.JSON );
            case "BOOLEAN" -> typeFactory.createPolyType( PolyType.BOOLEAN );
            case "TINYINT" -> typeFactory.createPolyType( PolyType.TINYINT );
            case "SMALLINT" -> typeFactory.createPolyType( PolyType.SMALLINT );
            case "BIGINT" -> typeFactory.createPolyType( PolyType.BIGINT );
            case "DATE" -> typeFactory.createPolyType( PolyType.DATE );
            case "TIME" -> {
                if ( parameterMeta.getPrecision() >= 0 ) {
                    yield typeFactory.createPolyType( PolyType.TIME, parameterMeta.getPrecision() );
                }
                yield typeFactory.createPolyType( PolyType.TIME );
            }
            case "TIMESTAMP" -> {
                if ( parameterMeta.getPrecision() >= 0 ) {
                    yield typeFactory.createPolyType( PolyType.TIMESTAMP, parameterMeta.getPrecision() );
                }
                yield typeFactory.createPolyType( PolyType.TIMESTAMP );
            }
            case "BINARY" -> {
                if ( parameterMeta.getPrecision() > 0 ) {
                    yield typeFactory.createPolyType( PolyType.BINARY, parameterMeta.getPrecision() );
                }
                yield typeFactory.createPolyType( PolyType.BINARY );
            }
            case "VARBINARY" -> {
                if ( parameterMeta.getPrecision() > 0 ) {
                    yield typeFactory.createPolyType( PolyType.VARBINARY, parameterMeta.getPrecision() );
                }
                yield typeFactory.createPolyType( PolyType.VARBINARY );
            }
            case "FILE" -> typeFactory.createPolyType( PolyType.FILE );
            case "IMAGE" -> typeFactory.createPolyType( PolyType.IMAGE );
            case "VIDEO" -> typeFactory.createPolyType( PolyType.VIDEO );
            case "AUDIO" -> typeFactory.createPolyType( PolyType.AUDIO );
            default -> typeFactory.createPolyType( PolyType.valueOf( parameterMeta.getTypeName() ) );
        };
    }


    @Override
    public void close() {
        statement.close();
        closeResults();
    }


    @Override
    public Transaction getTransaction() {
        return statement.getTransaction();
    }

}

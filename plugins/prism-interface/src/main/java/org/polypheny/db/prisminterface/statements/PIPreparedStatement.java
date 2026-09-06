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

import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.prisminterface.PIClient;
import org.polypheny.db.prisminterface.statementProcessing.StatementProcessor;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.VectorType;
import org.polypheny.prism.ParameterMeta;

@Setter
public abstract class PIPreparedStatement extends PIStatement implements Signaturizable {

    protected List<ParameterMeta> parameterMetas;
    @Getter
    protected List<AlgDataType> parameterPolyTypes;


    public List<ParameterMeta> getParameterMetas() {
        if ( parameterMetas == null ) {
            StatementProcessor.prepare( this );
        }
        return parameterMetas;
    }


    protected PIPreparedStatement(
            int id,
            @NotNull PIClient client,
            @NotNull QueryLanguage language,
            @NotNull LogicalNamespace namespace ) {
        super( id, client, language, namespace );
    }


    protected AlgDataType deriveType( JavaTypeFactory typeFactory, AlgDataType parameterMeta ) {
        PolyType type = parameterMeta.getPolyType();
        return switch ( type ) {
            case DECIMAL -> {
                if ( parameterMeta.getPrecision() >= 0 && parameterMeta.getScale() >= 0 ) {
                    yield typeFactory.createPolyType( PolyType.DECIMAL, parameterMeta.getPrecision(), parameterMeta.getScale() );
                } else if ( parameterMeta.getPrecision() >= 0 ) {
                    yield typeFactory.createPolyType( PolyType.DECIMAL, parameterMeta.getPrecision() );
                }
                yield typeFactory.createPolyType( PolyType.DECIMAL );
            }
            case VARCHAR -> {
                if ( parameterMeta.getPrecision() > 0 ) {
                    yield typeFactory.createPolyType( PolyType.VARCHAR, parameterMeta.getPrecision() );
                }
                yield typeFactory.createPolyType( PolyType.VARCHAR );
            }
            case CHAR -> {
                if ( parameterMeta.getPrecision() > 0 ) {
                    yield typeFactory.createPolyType( PolyType.CHAR, parameterMeta.getPrecision() );
                }
                yield typeFactory.createPolyType( PolyType.CHAR );
            }
            case TIME -> {
                if ( parameterMeta.getPrecision() >= 0 ) {
                    yield typeFactory.createPolyType( PolyType.TIME, parameterMeta.getPrecision() );
                }
                yield typeFactory.createPolyType( PolyType.TIME );
            }
            case TIMESTAMP -> {
                if ( parameterMeta.getPrecision() >= 0 ) {
                    yield typeFactory.createPolyType( PolyType.TIMESTAMP, parameterMeta.getPrecision() );
                }
                yield typeFactory.createPolyType( PolyType.TIMESTAMP );
            }
            case BINARY -> {
                if ( parameterMeta.getPrecision() > 0 ) {
                    yield typeFactory.createPolyType( PolyType.BINARY, parameterMeta.getPrecision() );
                }
                yield typeFactory.createPolyType( PolyType.BINARY );
            }
            case VARBINARY -> {
                if ( parameterMeta.getPrecision() > 0 ) {
                    yield typeFactory.createPolyType( PolyType.VARBINARY, parameterMeta.getPrecision() );
                }
                yield typeFactory.createPolyType( PolyType.VARBINARY );
            }
            case ARRAY -> {
                Optional<VectorType> vt = parameterMeta.unwrap( VectorType.class );
                if ( vt.isPresent() ) {
                    yield vt.get();
                }
                Optional<ArrayType> at = parameterMeta.unwrap( ArrayType.class );
                if ( at.isPresent() ) {
                    yield typeFactory.createArrayType(
                            typeFactory.createPolyType(
                                    at.get().getComponentType().getPolyType() ),
                            at.get().getCardinality(),
                            at.get().getDimension() );
                }
                yield typeFactory.createPolyType( type );
            }
            default -> typeFactory.createPolyType( type );
        };
    }


}

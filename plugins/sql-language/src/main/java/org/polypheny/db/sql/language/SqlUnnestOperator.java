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

package org.polypheny.db.sql.language;


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.fun.UnnestOperator;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.MapPolyType;
import org.polypheny.db.type.MultisetPolyType;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.Util;


/**
 * The <code>UNNEST</code> operator.
 */
public class SqlUnnestOperator extends SqlFunctionalOperator implements UnnestOperator {

    /**
     * Whether {@code WITH ORDINALITY} was specified.
     *
     * If so, the returned records include a column {@code ORDINALITY}.
     */
    public final boolean withOrdinality;


    public SqlUnnestOperator( boolean withOrdinality ) {
        super(
                "UNNEST",
                Kind.UNNEST,
                200,
                true,
                null,
                null,
                OperandTypes.repeat(
                        PolyOperandCountRanges.from( 1 ),
                        OperandTypes.SCALAR_OR_RECORD_COLLECTION_OR_MAP ) );
        this.withOrdinality = withOrdinality;
    }


    @Override
    public AlgDataType inferReturnType( OperatorBinding opBinding ) {
        final AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final AlgDataTypeFactory.Builder builder = typeFactory.builder();
        for ( Integer operand : Util.range( opBinding.getOperandCount() ) ) {
            AlgDataType type = opBinding.getOperandType( operand );
            if ( type.getPolyType() == PolyType.ANY ) {
                // Unnest Operator in schema less systems returns one column as the output $unnest is a place holder to specify that one column with type ANY is output.
                return builder
                        .add( "$unnest", null, PolyType.ANY )
                        .nullable( true )
                        .build();
            }

            if ( type.isStruct() ) {
                type = type.getFields().get( 0 ).getType();
            }

            assert type instanceof ArrayType || type instanceof MultisetPolyType || type instanceof MapPolyType;
            if ( type instanceof MapPolyType ) {
                builder.add( null, UnnestOperator.MAP_KEY_COLUMN_NAME, null, type.unwrap( MapPolyType.class ).orElseThrow().getKeyType() );
                builder.add( null, UnnestOperator.MAP_VALUE_COLUMN_NAME, null, type.unwrap( MapPolyType.class ).orElseThrow().getValueType() );
            } else {
                if ( type.getComponentType().isStruct() ) {
                    builder.addAll( type.getComponentType().getFields() );
                } else {
                    builder.add( null, CoreUtil.deriveAliasFromOrdinal( operand ), null, type.getComponentType() );
                }
            }
        }
        if ( withOrdinality ) {
            builder.add( UnnestOperator.ORDINALITY_COLUMN_NAME, null, PolyType.INTEGER );
        }
        return builder.build();
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        super.unparse( writer, call, leftPrec, rightPrec );
        if ( withOrdinality ) {
            writer.keyword( "WITH ORDINALITY" );
        }
    }


    @Override
    public boolean argumentMustBeScalar( int ordinal ) {
        return false;
    }

}


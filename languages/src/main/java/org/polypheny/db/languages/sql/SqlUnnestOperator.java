/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.languages.sql;


import org.polypheny.db.core.Kind;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.MapPolyType;
import org.polypheny.db.type.MultisetPolyType;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.util.Util;


/**
 * The <code>UNNEST</code> operator.
 */
public class SqlUnnestOperator extends SqlFunctionalOperator {

    /**
     * Whether {@code WITH ORDINALITY} was specified.
     *
     * If so, the returned records include a column {@code ORDINALITY}.
     */
    public final boolean withOrdinality;

    public static final String ORDINALITY_COLUMN_NAME = "ORDINALITY";

    public static final String MAP_KEY_COLUMN_NAME = "KEY";

    public static final String MAP_VALUE_COLUMN_NAME = "VALUE";


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
    public RelDataType inferReturnType( SqlOperatorBinding opBinding ) {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final RelDataTypeFactory.Builder builder = typeFactory.builder();
        for ( Integer operand : Util.range( opBinding.getOperandCount() ) ) {
            RelDataType type = opBinding.getOperandType( operand );
            if ( type.getPolyType() == PolyType.ANY ) {
                // Unnest Operator in schema less systems returns one column as the output $unnest is a place holder to specify that one column with type ANY is output.
                return builder
                        .add( "$unnest", null, PolyType.ANY )
                        .nullable( true )
                        .build();
            }

            if ( type.isStruct() ) {
                type = type.getFieldList().get( 0 ).getType();
            }

            assert type instanceof ArrayType || type instanceof MultisetPolyType || type instanceof MapPolyType;
            if ( type instanceof MapPolyType ) {
                builder.add( MAP_KEY_COLUMN_NAME, null, type.getKeyType() );
                builder.add( MAP_VALUE_COLUMN_NAME, null, type.getValueType() );
            } else {
                if ( type.getComponentType().isStruct() ) {
                    builder.addAll( type.getComponentType().getFieldList() );
                } else {
                    builder.add( SqlUtil.deriveAliasFromOrdinal( operand ), null, type.getComponentType() );
                }
            }
        }
        if ( withOrdinality ) {
            builder.add( ORDINALITY_COLUMN_NAME, null, PolyType.INTEGER );
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


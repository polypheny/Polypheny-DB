/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.adapter.cottontail;


import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.enumerable.EnumerableRel;
import org.polypheny.db.adapter.enumerable.EnumerableRelImplementor;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitDef;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.convert.ConverterImpl;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BuiltInMethod;


public class CottontailToEnumerableConverter extends ConverterImpl implements EnumerableRel {

    public static final List<PolyType> SUPPORTED_ARRAY_COMPONENT_TYPES = ImmutableList.of(
            PolyType.INTEGER,
            PolyType.DOUBLE,
            PolyType.FLOAT,
            PolyType.REAL,
            PolyType.BIGINT,
            PolyType.BOOLEAN );

    /**
     * Creates a ConverterImpl.
     *
     * @param cluster planner's cluster
     * @param traitDef the RelTraitDef this converter converts
     * @param traits the output traits of this converter
     * @param child child rel (provides input traits)
     */
    protected CottontailToEnumerableConverter( RelOptCluster cluster, RelTraitDef traitDef, RelTraitSet traits, RelNode child ) {
        super( cluster, traitDef, traits, child );
    }


    @Override
    public Result implement( EnumerableRelImplementor implementor, Prefer pref ) {
        return null;
    }


    private void generateGet(
            RelDataType rowType,
            List<String> physicalRowNames,
            BlockBuilder blockBuilder,
            ParameterExpression result_, /* This should be a DataMap<String, Data> */
            int i,
            Expression target
    ) {
        final RelDataType fieldType = rowType.getFieldList().get( i ).getType();

        // Fetch Data from DataMap
        // This should generate: `result_.get(<physical field name>)`
        final Expression getDataFromMap_ = Expressions.call( result_, "get", Expressions.constant( physicalRowNames.get( i ) ) );

        final Expression source;

        //x Bigint
        //x Boolean
        // Date
        //x Decimal -> BigDecimal STRING
        //x Double
        //x Integer
        //x Real -> FLOAT
        // Time
        // Timestamp
        //x Varchar
        //X Char -> STRING
        // Varbinary
        // Binary

        // ARRAYS:
        //x Bigint -> LongVector
        // Boolean -> BoolVector
        // Date
        // Decimal
        // Double -> DoubleVector
        // Integer -> IntVector
        // Real
        // Time
        // Timestamp
        // Varchar
        // Char
        // Varbinary
        // Binary

        switch ( fieldType.getPolyType() ) {
            case BOOLEAN:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case REAL:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
            case NULL:
                /* These are all simple types to fetch from the result. */
                source = Expressions.call( getDataFromMap_, cottontailGetMethod( fieldType.getPolyType() ) );
//                source = Expressions.call( getDataFromMap_, cottontailGetMethod( fieldType.getPolyType() ), Expressions.constant( i + 1 ) );
                break;
            case DECIMAL:
                // Polypheny uses BigDecimal internally to represent DECIMAL values.
                // BigDecimal#toString gives an exact and unique representation of the value.
                // Reversal is: new BigDecimal(<string>)
                source = Expressions.call(
                        Types.lookupMethod( CottontailToEnumerableConverter.class, "bigDecimalFromString", String.class ),
                        Expressions.call( getDataFromMap_, "getStringData" ) );
                break;
            case BINARY:
            case VARBINARY:
                source = Expressions.call(
                        Types.lookupMethod( ByteString.class, "parseBase64", String.class ),
                        Expressions.call( getDataFromMap_, "getStringData" ) );
                break;
            case ARRAY: {
                ArrayType arrayType = (ArrayType) fieldType;
                if ( arrayType.getDimension() == 1 && SUPPORTED_ARRAY_COMPONENT_TYPES.contains( arrayType.getComponentType().getPolyType() ) ) {
                    // Cottontail supports flat arrays natively
                    source = Expressions.call(
                            Expressions.call(
                                    getDataFromMap_,
                                    cottontailGetVectorMethod( arrayType.getComponentType().getPolyType() ) ),
                            "getVectorList" );
                } else {
                    // We need to handle nested arrays
                    source = Expressions.call(
                            BuiltInMethod.JDBC_PARSE_ARRAY_FROM_TEXT.method,
                            Expressions.constant( fieldType.getComponentType().getPolyType() ),
                            Expressions.constant( arrayType.getDimension() ),
                            Expressions.call( getDataFromMap_, "getStringData" ) );
                }
            }
            break;
            default:
                throw new AssertionError( "Not yet supported type: " + fieldType.getPolyType() );

        }

        blockBuilder.add( Expressions.statement( Expressions.assign( target, source ) ) );
    }


    private String cottontailGetMethod( PolyType polyType ) {
        switch ( polyType ) {
            case BOOLEAN:
                return "getBooleanData";
            case INTEGER:
                return "getIntData";
            case BIGINT:
                return "getLongData";
            case FLOAT:
            case REAL: // We are mapping REAL to CT FLOAT
                return "getFloatData";
            case DOUBLE:
                return "getDoubleData";
            case CHAR:
            case VARCHAR:
                return "getStringData";
            case NULL:
                return "getNullData";
            case TINYINT:
            case SMALLINT:
            case DECIMAL:
            case BINARY:
            case VARBINARY:
            case ANY:
            default:
                throw new AssertionError( "No primitive access method for type: " + polyType );
        }
    }


    private String cottontailGetVectorMethod( PolyType polyType ) {
        switch ( polyType ) {
            case INTEGER:
                return "getIntVector";
            case FLOAT:
            case REAL:
                return "getFloatVector";
            case DOUBLE:
                return "getDoubleVector";
            case BIGINT:
                return "getLongVector";
            default:
                throw new AssertionError( "No vector access method for inner type: " + polyType );
        }
    }


    public static BigDecimal bigDecimalFromString( final String string ) {
        return new BigDecimal( string );
    }
}

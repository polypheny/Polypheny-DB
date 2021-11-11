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

package org.polypheny.db.adapter.cottontail;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.cottontail.enumberable.CottontailDeleteEnumerable;
import org.polypheny.db.adapter.cottontail.enumberable.CottontailEnumerableFactory;
import org.polypheny.db.adapter.cottontail.rel.CottontailRel.CottontailImplementContext;
import org.polypheny.db.adapter.cottontail.util.Linq4JFixer;
import org.polypheny.db.adapter.enumerable.EnumerableRel;
import org.polypheny.db.adapter.enumerable.EnumerableRelImplementor;
import org.polypheny.db.adapter.enumerable.JavaRowFormat;
import org.polypheny.db.adapter.enumerable.PhysType;
import org.polypheny.db.adapter.enumerable.PhysTypeImpl;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.AbstractRelNode;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.convert.ConverterImpl;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;
import org.vitrivr.cottontail.client.iterators.Tuple;


public class CottontailToEnumerableConverter extends ConverterImpl implements EnumerableRel {

    public static final List<PolyType> SUPPORTED_ARRAY_COMPONENT_TYPES = ImmutableList.of(
            PolyType.TINYINT,
            PolyType.SMALLINT,
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
     * @param traits the output traits of this converter
     * @param child child rel (provides input traits)
     */
    public CottontailToEnumerableConverter( RelOptCluster cluster, RelTraitSet traits, RelNode child ) {
        super( cluster, ConventionTraitDef.INSTANCE, traits, child );
    }


    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return new CottontailToEnumerableConverter( getCluster(), traitSet, AbstractRelNode.sole( inputs ) );
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( .1 );
    }


    @Override
    public Result implement( EnumerableRelImplementor implementor, Prefer pref ) {
        final BlockBuilder list = new BlockBuilder();
        final CottontailImplementContext cottontailContext = new CottontailImplementContext();
        cottontailContext.blockBuilder = list;
        cottontailContext.visitChild( 0, getInput() );

        final CottontailConvention convention = (CottontailConvention) getInput().getConvention();
        final RelDataType rowType = getRowType();
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), rowType, pref.prefer( JavaRowFormat.ARRAY ) );

        List<Pair<String, String>> pairs = Pair.zip(
                rowType.getFieldList().stream().map( RelDataTypeField::getPhysicalName ).collect( Collectors.toList() ),
                rowType.getFieldNames() );
        List<String> physicalFieldNames = pairs.stream()
                .map( it -> it.left != null ? it.left : it.right )
                .collect( Collectors.toList() );

        final Expression enumerable;

        switch ( cottontailContext.queryType ) {
            case SELECT:
                // Row Parser
                final int fieldCount = getRowType().getFieldCount();
                BlockBuilder builder = new BlockBuilder();

                final ParameterExpression resultMap_ = Expressions.parameter(
                        Tuple.class, builder.newName( "resultDataMap" )
                );

                if ( fieldCount == 1 ) {
                    final ParameterExpression value_ = Expressions.parameter( Object.class, builder.newName( "value" ) );
                    builder.add( Expressions.declare( Modifier.FINAL, value_, null ) );
                    this.generateGet( rowType, physicalFieldNames, builder, resultMap_, 0, value_ );
                    builder.add( Expressions.return_( null, value_ ) );
                } else {
                    final Expression values_ = builder.append(
                            "values",
                            Expressions.newArrayBounds( Object.class, 1, Expressions.constant( fieldCount ) ) );

                    for ( int i = 0; i < fieldCount; i++ ) {
                        this.generateGet(
                                rowType,
                                physicalFieldNames, // PHYSICAL ROW NAMES
                                builder,
                                resultMap_, // Parameter expr result
                                i,
                                Expressions.arrayIndex( values_, Expressions.constant( i ) )
                        );
                    }

                    builder.add( Expressions.return_( null, values_ ) );
                }

                final Expression rowBuilder_ = list.append( "rowBuilder", Expressions.lambda( Expressions.block( builder.toBlock() ), resultMap_ ) );
                enumerable = list.append( "enumerable",
                        Expressions.call( CottontailEnumerableFactory.CREATE_QUERY_METHOD,
                                Expressions.constant( cottontailContext.tableName ),
                                Expressions.constant( cottontailContext.schemaName ),
                                expressionOrNullExpression( cottontailContext.projectionMap ), // PROJECTION
                                expressionOrNullExpression( cottontailContext.sortMap ), // ORDER BY
                                expressionOrNullExpression( cottontailContext.limitBuilder ), // LIMIT
                                expressionOrNullExpression( cottontailContext.offsetBuilder ), // OFFSET
                                expressionOrNullExpression( cottontailContext.filterBuilder ), // WHERE
                                DataContext.ROOT,
                                rowBuilder_,
                                Expressions.call( Schemas.unwrap( convention.expression, CottontailSchema.class ), "getWrapper" )
                        ) );
                break;

            case INSERT:
                if ( cottontailContext.valuesHashMapList != null ) {
                    enumerable = list.append( "enumerable",
                            Expressions.call( CottontailEnumerableFactory.CREATE_INSERT_VALUES,
                                    Expressions.constant( cottontailContext.tableName ),
                                    Expressions.constant( cottontailContext.schemaName ),
                                    cottontailContext.valuesHashMapList,
                                    DataContext.ROOT,
                                    Expressions.call( Schemas.unwrap( convention.expression, CottontailSchema.class ), "getWrapper" )
                            )
                    );
                } else {
                    enumerable = list.append( "enumerable",
                            Expressions.call( CottontailEnumerableFactory.CREATE_INSERT_PREPARED,
                                    Expressions.constant( cottontailContext.tableName ),
                                    Expressions.constant( cottontailContext.schemaName ),
                                    cottontailContext.preparedValuesMapBuilder,
                                    DataContext.ROOT,
                                    Expressions.call( Schemas.unwrap( convention.expression, CottontailSchema.class ), "getWrapper" )
                            )
                    );
                }
                break;

            case UPDATE:
                enumerable = list.append( "enumerable",
                        Expressions.call( CottontailEnumerableFactory.CREATE_UPDATE_METHOD,
                                Expressions.constant( cottontailContext.tableName ),
                                Expressions.constant( cottontailContext.schemaName ),
                                expressionOrNullExpression( cottontailContext.filterBuilder ),
                                cottontailContext.preparedValuesMapBuilder,
                                DataContext.ROOT,
                                Expressions.call( Schemas.unwrap( convention.expression, CottontailSchema.class ), "getWrapper" )
                        )
                );
                break;

            case DELETE:
                enumerable = list.append( "enumerable",
                        Expressions.call( CottontailDeleteEnumerable.CREATE_DELETE_METHOD,
                                Expressions.constant( cottontailContext.tableName ),
                                Expressions.constant( cottontailContext.schemaName ),
                                expressionOrNullExpression( cottontailContext.filterBuilder ),
                                DataContext.ROOT,
                                Expressions.call( Schemas.unwrap( convention.expression, CottontailSchema.class ), "getWrapper" )
                        )
                );
                break;

            default:
                enumerable = null;
        }

        list.add( Expressions.statement( Expressions.call(
                Schemas.unwrap( convention.expression, CottontailSchema.class ),
                "registerStore",
                DataContext.ROOT ) ) );

        list.add( Expressions.return_( null, enumerable ) );

        return implementor.result( physType, list.toBlock() );
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
        final Expression getDataFromMap_;
        try {
            getDataFromMap_ = blockBuilder.append( "v" + i, Expressions.call( result_, BuiltInMethod.MAP_GET.method, Expressions.constant( physicalRowNames.get( i ).toLowerCase() ) ) );
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }

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
            case JSON:
            case DECIMAL:
                // Polypheny uses BigDecimal internally to represent DECIMAL values.
                // BigDecimal#toString gives an exact and unique representation of the value.
                // Reversal is: new BigDecimal(<string>)
                // Decimal decoding is handled properly in Linq4JFixer
            case BINARY:
            case VARBINARY:
            case IMAGE:
            case SOUND:
            case VIDEO:
            case FILE:
                // Binary and VarBinary are turned into base64 string
                // Linq4JFixer takes care of decoding
            case TINYINT:
            case SMALLINT:
                // Handled by linq4jFixer
            case DATE:
            case TIME:
            case TIMESTAMP:
            case NULL:
                /* These are all simple types to fetch from the result. */
                source = Expressions.call( cottontailGetMethod( fieldType.getPolyType() ), getDataFromMap_ );
                break;
            case ARRAY: {
                ArrayType arrayType = (ArrayType) fieldType;
                if ( arrayType.getDimension() == 1 && SUPPORTED_ARRAY_COMPONENT_TYPES.contains( arrayType.getComponentType().getPolyType() ) ) {
                    // Cottontail supports flat arrays natively
                    source = Expressions.call(
                            cottontailGetVectorMethod( arrayType.getComponentType().getPolyType() ),
                            getDataFromMap_ );
                } else {
                    // We need to handle nested arrays
                    source = Expressions.call(
                            BuiltInMethod.PARSE_ARRAY_FROM_TEXT.method,
                            Expressions.constant( fieldType.getComponentType().getPolyType() ),
                            Expressions.constant( arrayType.getDimension() ),
                            Expressions.call( cottontailGetMethod( PolyType.VARCHAR ), getDataFromMap_ ) );
                }
            }
            break;
            default:
                throw new AssertionError( "Not yet supported type: " + fieldType.getPolyType() );

        }

        blockBuilder.add( Expressions.statement( Expressions.assign( target, source ) ) );
    }


    private Method cottontailGetMethod( PolyType polyType ) {
        switch ( polyType ) {
            case BOOLEAN:
                return Types.lookupMethod( Linq4JFixer.class, "getBooleanData", Object.class );
            case TINYINT:
                return Types.lookupMethod( Linq4JFixer.class, "getTinyIntData", Object.class );
            case SMALLINT:
                return Types.lookupMethod( Linq4JFixer.class, "getSmallIntData", Object.class );
            case INTEGER:
                return Types.lookupMethod( Linq4JFixer.class, "getIntData", Object.class );
            case BIGINT:
                return Types.lookupMethod( Linq4JFixer.class, "getLongData", Object.class );
            case FLOAT:
            case REAL: // We are mapping REAL to CT FLOAT
                return Types.lookupMethod( Linq4JFixer.class, "getFloatData", Object.class );
            case DOUBLE:
                return Types.lookupMethod( Linq4JFixer.class, "getDoubleData", Object.class );
            case CHAR:
            case VARCHAR:
            case JSON:
                return Types.lookupMethod( Linq4JFixer.class, "getStringData", Object.class );
            case NULL:
                return Types.lookupMethod( Linq4JFixer.class, "getNullData", Object.class );
            case DECIMAL:
                return Types.lookupMethod( Linq4JFixer.class, "getDecimalData", Object.class );
            case BINARY:
            case VARBINARY:
            case IMAGE:
            case SOUND:
            case VIDEO:
            case FILE:
                return Types.lookupMethod( Linq4JFixer.class, "getBinaryData", Object.class );
            case TIME:
                return Types.lookupMethod( Linq4JFixer.class, "getTimeData", Object.class );
            case DATE:
                return Types.lookupMethod( Linq4JFixer.class, "getDateData", Object.class );
            case TIMESTAMP:
                return Types.lookupMethod( Linq4JFixer.class, "getTimestampData", Object.class );
            case ANY:
            default:
                throw new AssertionError( "No primitive access method for type: " + polyType );
        }
    }


    private Method cottontailGetVectorMethod( PolyType polyType ) {
        switch ( polyType ) {
            case BOOLEAN:
                return Types.lookupMethod( Linq4JFixer.class, "getBoolVector", Object.class );
            case SMALLINT:
                return Types.lookupMethod( Linq4JFixer.class, "getSmallIntVector", Object.class );
            case TINYINT:
                return Types.lookupMethod( Linq4JFixer.class, "getTinyIntVector", Object.class );
            case INTEGER:
                return Types.lookupMethod( Linq4JFixer.class, "getIntVector", Object.class );
            case FLOAT:
            case REAL:
                return Types.lookupMethod( Linq4JFixer.class, "getFloatVector", Object.class );
            case DOUBLE:
                return Types.lookupMethod( Linq4JFixer.class, "getDoubleVector", Object.class );
            case BIGINT:
                return Types.lookupMethod( Linq4JFixer.class, "getLongVector", Object.class );
            default:
                throw new AssertionError( "No vector access method for inner type: " + polyType );
        }
    }


    public static BigDecimal bigDecimalFromString( final String string ) {
        return new BigDecimal( string );
    }


    private static Expression expressionOrNullExpression( Expression expression ) {
        if ( expression == null ) {
            return Expressions.constant( null );
        } else {
            return expression;
        }
    }

}
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

package org.polypheny.db.adapter.cottontail.algebra;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.cottontail.CottontailEntity;
import org.polypheny.db.adapter.cottontail.algebra.CottontailAlg.CottontailImplementContext;
import org.polypheny.db.adapter.cottontail.enumberable.CottontailDeleteEnumerable;
import org.polypheny.db.adapter.cottontail.enumberable.CottontailEnumerableFactory;
import org.polypheny.db.adapter.cottontail.util.Linq4JFixer;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterImpl;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.JavaTupleFormat;
import org.polypheny.db.algebra.enumerable.PhysType;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.BuiltInMethod;
import org.vitrivr.cottontail.client.iterators.Tuple;

public class CottontailToEnumerableConverter extends ConverterImpl implements EnumerableAlg {

    public static final List<PolyType> SUPPORTED_ARRAY_COMPONENT_TYPES = ImmutableList.of(
            PolyType.TINYINT,
            PolyType.SMALLINT,
            PolyType.INTEGER,
            PolyType.DOUBLE,
            PolyType.DECIMAL,
            PolyType.FLOAT,
            PolyType.REAL,
            PolyType.BIGINT,
            PolyType.BOOLEAN );


    /**
     * Creates a ConverterImpl.
     *
     * @param cluster planner's cluster
     * @param traits the output traits of this converter
     * @param child child alg (provides input traits)
     */
    public CottontailToEnumerableConverter( AlgCluster cluster, AlgTraitSet traits, AlgNode child ) {
        super( cluster, ConventionTraitDef.INSTANCE, traits, child );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new CottontailToEnumerableConverter( getCluster(), traitSet, AbstractAlgNode.sole( inputs ) );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( .1 );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder list = new BlockBuilder();
        final CottontailImplementContext cottontailContext = new CottontailImplementContext();
        cottontailContext.blockBuilder = list;
        cottontailContext.visitChild( 0, getInput() );

        final AlgDataType rowType = getTupleType();
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), rowType, pref.prefer( JavaTupleFormat.ARRAY ) );
        final Expression enumerable;

        switch ( cottontailContext.queryType ) {
            case SELECT:
                // Row Parser
                final int fieldCount = getTupleType().getFieldCount();
                BlockBuilder builder = new BlockBuilder();

                final ParameterExpression resultMap_ = Expressions.parameter(
                        Tuple.class,
                        builder.newName( "resultDataMap" ) );

                final Expression values_ = builder.append(
                        "values",
                        Expressions.newArrayBounds( PolyValue.class, 1, Expressions.constant( fieldCount ) ) );
                for ( int i = 0; i < fieldCount; i++ ) {
                    if ( cottontailContext.dynamicParams.containsKey( getTupleType().getFieldNames().get( i ) ) ) {
                        this.generateDynamicGet( builder, cottontailContext.dynamicParams.get( getTupleType().getFieldNames().get( i ) ), Expressions.arrayIndex( values_, Expressions.constant( i ) ) );
                        continue;
                    }

                    this.generateGet( rowType, builder, resultMap_, i, Expressions.arrayIndex( values_, Expressions.constant( i ) ) );
                }
                builder.add( Expressions.return_( null, values_ ) );

                final Expression rowBuilder_ = list.append(
                        "rowBuilder",
                        Expressions.lambda( Expressions.block( builder.toBlock() ), resultMap_ ) );
                enumerable = list.append(
                        "enumerable",
                        Expressions.call(
                                CottontailEnumerableFactory.CREATE_QUERY_METHOD,
                                Expressions.constant( cottontailContext.tableName ),
                                Expressions.constant( cottontailContext.schemaName ),
                                expressionOrNullExpression( cottontailContext.projectionMap ), // PROJECTION
                                expressionOrNullExpression( cottontailContext.sortMap ), // ORDER BY
                                expressionOrNullExpression( cottontailContext.limitBuilder ), // LIMIT
                                expressionOrNullExpression( cottontailContext.offsetBuilder ), // OFFSET
                                expressionOrNullExpression( cottontailContext.filterBuilder ), // WHERE
                                DataContext.ROOT,
                                rowBuilder_,
                                cottontailContext.table.asExpression( CottontailEntity.class )
                        ) );
                break;

            case INSERT:
                if ( cottontailContext.valuesHashMapList != null ) {
                    enumerable = list.append(
                            "enumerable",
                            Expressions.call(
                                    CottontailEnumerableFactory.CREATE_INSERT_VALUES,
                                    Expressions.constant( cottontailContext.tableName ),
                                    Expressions.constant( cottontailContext.schemaName ),
                                    cottontailContext.valuesHashMapList,
                                    DataContext.ROOT,
                                    cottontailContext.table.asExpression( CottontailEntity.class )
                            )
                    );
                } else {
                    enumerable = list.append(
                            "enumerable",
                            Expressions.call(
                                    CottontailEnumerableFactory.CREATE_INSERT_PREPARED,
                                    Expressions.constant( cottontailContext.tableName ),
                                    Expressions.constant( cottontailContext.schemaName ),
                                    cottontailContext.preparedValuesMapBuilder,
                                    DataContext.ROOT,
                                    cottontailContext.table.asExpression( CottontailEntity.class )
                            )
                    );
                }
                break;

            case UPDATE:
                enumerable = list.append(
                        "enumerable",
                        Expressions.call(
                                CottontailEnumerableFactory.CREATE_UPDATE_METHOD,
                                Expressions.constant( cottontailContext.tableName ),
                                Expressions.constant( cottontailContext.schemaName ),
                                expressionOrNullExpression( cottontailContext.filterBuilder ),
                                cottontailContext.preparedValuesMapBuilder,
                                DataContext.ROOT,
                                cottontailContext.table.asExpression( CottontailEntity.class )
                        )
                );
                break;

            case DELETE:
                enumerable = list.append(
                        "enumerable",
                        Expressions.call(
                                CottontailDeleteEnumerable.CREATE_DELETE_METHOD,
                                expressionOrNullExpression( cottontailContext.filterBuilder ),
                                DataContext.ROOT,
                                cottontailContext.table.asExpression( CottontailEntity.class )
                        )
                );
                break;

            default:
                enumerable = null;
        }

        list.add( Expressions.statement(
                Expressions.call(
                        cottontailContext.table.asExpression( CottontailEntity.class ),
                        "registerStore",
                        DataContext.ROOT ) ) );

        list.add( Expressions.return_( null, enumerable ) );

        return implementor.result( physType, list.toBlock() );
    }


    private void generateDynamicGet( BlockBuilder builder, Long dynamicIndex, Expression target ) {
        builder.add( Expressions.statement( Expressions.assign( target, Expressions.call( CottontailEnumerableFactory.class, "getDynamicValue", DataContext.ROOT, Expressions.constant( dynamicIndex ) ) ) ) );
    }


    /**
     * Generates accessor methods used to access data contained in {@link Tuple}s returned by Cottontail DB.
     */
    private void generateGet( AlgDataType rowType, BlockBuilder blockBuilder, ParameterExpression result_, int i, Expression target ) {
        final AlgDataType fieldType = rowType.getFields().get( i ).getType();

        // Fetch Data from DataMap
        // This should generate: `result_.get(<physical field name>)`
        final Expression getDataFromMap_;
        try {
            getDataFromMap_ = blockBuilder.append( "v" + i, Expressions.call( result_, Types.lookupMethod( Tuple.class, "get", Integer.TYPE ), Expressions.constant( i ) ) );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( e );
        }

        // Create accessor + converter for values returned by Cottontail DB. */
        final Expression source;
        switch ( fieldType.getPolyType() ) {
            case BOOLEAN:
                source = Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getBoolData", Object.class ), getDataFromMap_ );
                break;
            case INTEGER:
                source = Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getIntData", Object.class ), getDataFromMap_ );
                break;
            case BIGINT:
                source = Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getBigIntData", Object.class ), getDataFromMap_ );
                break;
            case FLOAT:
                source = Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getFloatData", Object.class ), getDataFromMap_ );
                break;
            case REAL:
                source = Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getRealData", Object.class ), getDataFromMap_ );
                break;
            case DOUBLE:
                source = Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getDoubleData", Object.class ), getDataFromMap_ );
                break;
            case CHAR:
            case VARCHAR:
            case TEXT:
                source = Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getStringData", Object.class ), getDataFromMap_ );
                break;
            case NULL:
                source = getDataFromMap_;
                break;
            case TINYINT:
                source = Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getTinyIntData", Object.class ), getDataFromMap_ );
                break;
            case SMALLINT:
                source = Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getSmallIntData", Object.class ), getDataFromMap_ );
                break;
            case JSON:
                source = Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getStringData", Object.class ), getDataFromMap_ );
                break;
            case DECIMAL:
                // Polypheny uses BigDecimal internally to represent DECIMAL values.
                // BigDecimal#toString gives an exact and unique representation of the value.
                // Reversal is: new BigDecimal(<string>)
                // Decimal decoding is handled properly in Linq4JFixer
                source = Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getDecimalData", Object.class ), getDataFromMap_ );
                break;
            case DATE:
                source = Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getDateData", Object.class ), getDataFromMap_ );
                break;
            case TIME:
                source = Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getTimeData", Object.class ), getDataFromMap_ );
                break;
            case TIMESTAMP:
                source = Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getTimestampData", Object.class ), getDataFromMap_ );
                break;
            case BINARY:
            case VARBINARY:
            case IMAGE:
            case AUDIO:
            case VIDEO:
            case FILE:
                // Binary and VarBinary are turned into base64 string
                // Linq4JFixer takes care of decoding
                source = Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getBinaryData", Object.class ), getDataFromMap_ );
                break;
            case ARRAY: {
                ArrayType arrayType = (ArrayType) fieldType;
                if ( arrayType.getDimension() == 1 && SUPPORTED_ARRAY_COMPONENT_TYPES.contains( arrayType.getComponentType().getPolyType() ) ) {
                    source = switch ( arrayType.getComponentType().getPolyType() ) {
                        case BOOLEAN -> Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getBoolVector", Object.class ), getDataFromMap_ );
                        case SMALLINT -> Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getSmallIntVector", Object.class ), getDataFromMap_ );
                        case TINYINT -> Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getTinyIntVector", Object.class ), getDataFromMap_ );
                        case INTEGER -> Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getIntVector", Object.class ), getDataFromMap_ );
                        case FLOAT, REAL -> Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getFloatVector", Object.class ), getDataFromMap_ );
                        case DOUBLE, DECIMAL -> Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getDoubleVector", Object.class ), getDataFromMap_ );
                        case BIGINT -> Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getLongVector", Object.class ), getDataFromMap_ );
                        default -> throw new AssertionError( "No vector access method for inner type: " + arrayType.getPolyType() );
                    };
                } else {
                    source = Expressions.call(
                            BuiltInMethod.PARSE_ARRAY_FROM_TEXT.method,
                            Expressions.call( Types.lookupMethod( Linq4JFixer.class, "getStringData", Object.class ), getDataFromMap_ )
                    );
                }
            }
            break;
            default:
                throw new AssertionError( "Not yet supported type: " + fieldType.getPolyType() );

        }

        blockBuilder.add( Expressions.statement( Expressions.assign( target, source ) ) );
    }


    private static Expression expressionOrNullExpression( Expression expression ) {
        if ( expression == null ) {
            return Expressions.constant( null );
        } else {
            return expression;
        }
    }

}
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

package org.polypheny.db.adapter.file;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.file.Value.DynamicValue;
import org.polypheny.db.adapter.file.Value.InputValue;
import org.polypheny.db.adapter.file.Value.LiteralValue;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyValue;


public class Condition {

    private final Kind operator;

    private List<Value> values = new ArrayList<>();
    private final List<Condition> operands = new ArrayList<>();


    public Condition( final RexCall call ) {
        this.operator = call.getOperator().getKind();
        for ( RexNode rex : call.getOperands() ) {
            if ( rex instanceof RexCall ) {
                this.operands.add( new Condition( (RexCall) rex ) );
            } else if ( rex instanceof RexLiteral ) {
                this.values.add( new LiteralValue( null, ((RexLiteral) rex).value ) );
            } else if ( rex instanceof RexIndexRef ) {
                this.values.add( new InputValue( null, ((RexIndexRef) rex).getIndex() ) );
            } else if ( rex instanceof RexDynamicParam ) {
                this.values.add( new DynamicValue( null, ((RexDynamicParam) rex).getIndex() ) );
            } else {
                throw new GenericRuntimeException( "Unsupported RexNode type: " + rex.getClass().getName() );
            }
        }
    }


    public Condition( final RexDynamicParam dynamicParam ) {
        this.operator = Kind.DYNAMIC_PARAM;
        this.values = List.of( new DynamicValue( null, dynamicParam.getIndex() ) );
    }


    @SuppressWarnings("unused")
    public Condition( final Kind operator, final List<Condition> operands, final List<Value> values ) {
        this.operator = operator;
        this.values.addAll( values );
        this.operands.addAll( operands );
    }


    public static Condition create( RexNode node ) {
        if ( node instanceof RexCall call ) {
            return new Condition( call );
        } else if ( node instanceof RexDynamicParam dynamicParam ) {
            return new Condition( dynamicParam );
        }
        throw new GenericRuntimeException( "Unsupported RexNode type: " + node.getClass().getName() );
    }


    /**
     * For linq4j Expressions
     */
    public Expression getExpression() {

        return Expressions.new_(
                Condition.class,
                Expressions.constant( operator ),
                EnumUtils.constantArrayList( operands.stream().map( Condition::getExpression ).toList(), Condition.class ),
                EnumUtils.constantArrayList( values.stream().map( Value::asExpression ).toList(), Value.class ) );
    }


    /**
     * Determines if a condition is a primary key condition, i.e. an AND-condition over all primary key columns
     *
     * @param pkColumnReferences One-based references of the PK columns, e.g. [1,3] for [a,b,c] if a and c are the primary key columns
     * @param colSize Number of columns in the current query, needed to generate the object that will be hashed
     * @return {@code Null} if it is not a PK lookup, or an Object array with the lookups to hash, if it is a PK lookup
     */
    @Nullable
    public List<PolyValue> extractPks( final List<Integer> pkColumnReferences, final List<AlgDataTypeField> columnTypes, final int colSize, final DataContext dataContext ) {
        List<PolyValue> lookups = new ArrayList<>( Collections.nCopies( colSize, null ) );

        for ( Condition operand : operands ) {
            List<PolyValue> operandLookups = operand.extractPks( pkColumnReferences, columnTypes, colSize, dataContext );
            if ( operandLookups == null ) {
                return null;
            }
            for ( int i = 0; i < operandLookups.size(); i++ ) {
                if ( operandLookups.get( i ) != null ) {
                    lookups.set( i, operandLookups.get( i ) );
                }
            }
        }

        for ( Integer refIndex : getInputRefs() ) {
            if ( pkColumnReferences.contains( refIndex ) ) {
                PolyValue value = PolyLong.of( pkColumnReferences.get( refIndex ) );
                lookups.set( refIndex, value );
            }
        }

        if ( lookups.stream().allMatch( Objects::isNull ) ) {
            return null;
        }
        return lookups;
    }


    private List<Integer> getInputRefs() {
        return values.stream().filter( v -> v instanceof InputValue ).map( v -> ((InputValue) v).getIndex() ).toList();
    }


    public boolean matches( final List<PolyValue> columnValues, final List<AlgDataTypeField> columnTypes, final DataContext dataContext ) {

        if ( !operands.isEmpty() ) { // || literalIndex == null ) {
            return switch ( operator ) {
                case AND -> {
                    for ( Condition c : operands ) {
                        if ( !c.matches( columnValues, columnTypes, dataContext ) ) {
                            yield false;
                        }
                    }
                    yield true;
                }
                case OR -> {
                    for ( Condition c : operands ) {
                        if ( c.matches( columnValues, columnTypes, dataContext ) ) {
                            yield true;
                        }
                    }
                    yield false;
                }
                default -> throw new GenericRuntimeException( operator + " not supported in condition without columnReference" );
            };
        }

        List<PolyValue> parameterValues = values.stream().map( v -> v.getValue( columnValues, dataContext, 0 ) ).toList();
        Integer comparison = null;
        PolyValue value = parameterValues.get( 0 );
        if ( parameterValues.size() == 1 ) {
            switch ( operator ) {
                case IS_NULL:
                    return value == null || value.isNull();
                case IS_NOT_NULL:
                    return value != null && !value.isNull();
                case DYNAMIC_PARAM:
                    if ( value == null || value.isNull() ) {
                        return false;
                    }
                    return value.asBoolean().value;
            }

            if ( values.size() == 1 && value == null || value.isNull() ) {
                //WHERE x = null is always false
                return false;
            }
        } else if ( parameterValues.size() == 2 ) {
            comparison = value.compareTo( parameterValues.get( 1 ) );
        } else {
            throw new GenericRuntimeException( "Unsupported number of values in condition: " + values.size() );
        }

        if ( comparison == null ) {
            return false;
        }

        return switch ( operator ) {
            case AND -> {
                for ( Condition c : operands ) {
                    if ( !c.matches( columnValues, columnTypes, dataContext ) ) {
                        yield false;
                    }
                }
                yield true;
            }
            case OR -> {
                for ( Condition c : operands ) {
                    if ( c.matches( columnValues, columnTypes, dataContext ) ) {
                        yield true;
                    }
                }
                yield false;
            }

            case EQUALS -> comparison == 0;
            case NOT_EQUALS -> comparison != 0;
            case GREATER_THAN -> comparison > 0;
            case GREATER_THAN_OR_EQUAL -> comparison >= 0;
            case LESS_THAN -> comparison < 0;
            case LESS_THAN_OR_EQUAL -> comparison <= 0;
            default -> throw new GenericRuntimeException( operator + " comparison not supported by file adapter." );
        };
    }


    public void adjust( Value[] projectionMapping ) {
        operands.forEach( operand -> operand.adjust( projectionMapping ) );
        values.forEach( value -> value.adjust( List.of( projectionMapping ) ) );
    }


}

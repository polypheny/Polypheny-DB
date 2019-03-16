/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.plan;


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalFilter;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexCall;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexInputRef;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlCastFunction;
import ch.unibas.dmi.dbis.polyphenydb.util.NlsString;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.trace.PolyphenyDbLogger;
import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.linq4j.QueryProvider;
import org.slf4j.LoggerFactory;


/**
 * DataContext for evaluating an RexExpression
 */
public class VisitorDataContext implements DataContext {

    private static final PolyphenyDbLogger LOGGER = new PolyphenyDbLogger( LoggerFactory.getLogger( VisitorDataContext.class.getName() ) );

    private final Object[] values;


    public VisitorDataContext( Object[] values ) {
        this.values = values;
    }


    public SchemaPlus getRootSchema() {
        throw new RuntimeException( "Unsupported" );
    }


    public JavaTypeFactory getTypeFactory() {
        throw new RuntimeException( "Unsupported" );
    }


    public QueryProvider getQueryProvider() {
        throw new RuntimeException( "Unsupported" );
    }


    public Object get( String name ) {
        if ( name.equals( "inputRecord" ) ) {
            return values;
        } else {
            return null;
        }
    }


    public static DataContext of( RelNode targetRel, LogicalFilter queryRel ) {
        return of( targetRel.getRowType(), queryRel.getCondition() );
    }


    public static DataContext of( RelDataType rowType, RexNode rex ) {
        final int size = rowType.getFieldList().size();
        final Object[] values = new Object[size];
        final List<RexNode> operands = ((RexCall) rex).getOperands();
        final RexNode firstOperand = operands.get( 0 );
        final RexNode secondOperand = operands.get( 1 );
        final Pair<Integer, ?> value = getValue( firstOperand, secondOperand );
        if ( value != null ) {
            int index = value.getKey();
            values[index] = value.getValue();
            return new VisitorDataContext( values );
        } else {
            return null;
        }
    }


    public static DataContext of( RelDataType rowType, List<Pair<RexInputRef, RexNode>> usageList ) {
        final int size = rowType.getFieldList().size();
        final Object[] values = new Object[size];
        for ( Pair<RexInputRef, RexNode> elem : usageList ) {
            Pair<Integer, ?> value = getValue( elem.getKey(), elem.getValue() );
            if ( value == null ) {
                LOGGER.warn( "{} is not handled for {} for checking implication", elem.getKey(), elem.getValue() );
                return null;
            }
            int index = value.getKey();
            values[index] = value.getValue();
        }
        return new VisitorDataContext( values );
    }


    public static Pair<Integer, ?> getValue( RexNode inputRef, RexNode literal ) {
        inputRef = removeCast( inputRef );
        literal = removeCast( literal );

        if ( inputRef instanceof RexInputRef && literal instanceof RexLiteral ) {
            final int index = ((RexInputRef) inputRef).getIndex();
            final RexLiteral rexLiteral = (RexLiteral) literal;
            final RelDataType type = inputRef.getType();

            if ( type.getSqlTypeName() == null ) {
                LOGGER.warn( "{} returned null SqlTypeName", inputRef.toString() );
                return null;
            }

            switch ( type.getSqlTypeName() ) {
                case INTEGER:
                    return Pair.of( index, rexLiteral.getValueAs( Integer.class ) );
                case DOUBLE:
                    return Pair.of( index, rexLiteral.getValueAs( Double.class ) );
                case REAL:
                    return Pair.of( index, rexLiteral.getValueAs( Float.class ) );
                case BIGINT:
                    return Pair.of( index, rexLiteral.getValueAs( Long.class ) );
                case SMALLINT:
                    return Pair.of( index, rexLiteral.getValueAs( Short.class ) );
                case TINYINT:
                    return Pair.of( index, rexLiteral.getValueAs( Byte.class ) );
                case DECIMAL:
                    return Pair.of( index, rexLiteral.getValueAs( BigDecimal.class ) );
                case DATE:
                case TIME:
                    return Pair.of( index, rexLiteral.getValueAs( Integer.class ) );
                case TIMESTAMP:
                    return Pair.of( index, rexLiteral.getValueAs( Long.class ) );
                case CHAR:
                    return Pair.of( index, rexLiteral.getValueAs( Character.class ) );
                case VARCHAR:
                    return Pair.of( index, rexLiteral.getValueAs( String.class ) );
                default:
                    // TODO: Support few more supported cases
                    LOGGER.warn( "{} for value of class {} is being handled in default way", type.getSqlTypeName(), rexLiteral.getValue().getClass() );
                    if ( rexLiteral.getValue() instanceof NlsString ) {
                        return Pair.of( index, ((NlsString) rexLiteral.getValue()).getValue() );
                    } else {
                        return Pair.of( index, rexLiteral.getValue() );
                    }
            }
        }

        // Unsupported Arguments
        return null;
    }


    private static RexNode removeCast( RexNode inputRef ) {
        if ( inputRef instanceof RexCall ) {
            final RexCall castedRef = (RexCall) inputRef;
            final SqlOperator operator = castedRef.getOperator();
            if ( operator instanceof SqlCastFunction ) {
                inputRef = castedRef.getOperands().get( 0 );
            }
        }
        return inputRef;
    }
}


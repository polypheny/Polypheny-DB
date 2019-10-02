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

package ch.unibas.dmi.dbis.polyphenydb.sql.type;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCallBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperandCountRange;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ImmutableList;
import java.util.AbstractList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.calcite.linq4j.Ord;


/**
 * This class allows multiple existing {@link SqlOperandTypeChecker} rules to be combined into one rule. For example, allowing an operand to be either string or numeric could be done by:
 *
 * <blockquote>
 * <pre><code>
 * CompositeOperandsTypeChecking newCompositeRule =
 *     new CompositeOperandsTypeChecking(Composition.OR,
 *         new SqlOperandTypeChecker[]{stringRule, numericRule});
 * </code></pre>
 * </blockquote>
 *
 * Similarly a rule that would only allow a numeric literal can be done by:
 *
 * <blockquote>
 * <pre><code>
 * CompositeOperandsTypeChecking newCompositeRule =
 *     new CompositeOperandsTypeChecking(Composition.AND,
 *         new SqlOperandTypeChecker[]{numericRule, literalRule});
 * </code></pre>
 * </blockquote>
 *
 * Finally, creating a signature expecting a string for the first operand and a numeric for the second operand can be done by:
 *
 * <blockquote>
 * <pre><code>
 * CompositeOperandsTypeChecking newCompositeRule =
 *     new CompositeOperandsTypeChecking(Composition.SEQUENCE,
 *         new SqlOperandTypeChecker[]{stringRule, numericRule});
 * </code></pre>
 * </blockquote>
 *
 * For SEQUENCE composition, the rules must be instances of SqlSingleOperandTypeChecker, and signature generation is not supported. For AND composition, only the first rule is used for signature generation.
 */
public class CompositeOperandTypeChecker implements SqlOperandTypeChecker {

    private final SqlOperandCountRange range;


    /**
     * How operands are composed.
     */
    public enum Composition {
        AND, OR, SEQUENCE, REPEAT
    }


    protected final ImmutableList<? extends SqlOperandTypeChecker> allowedRules;
    protected final Composition composition;
    private final String allowedSignatures;


    /**
     * Package private. Use {@link OperandTypes#and}, {@link OperandTypes#or}.
     */
    CompositeOperandTypeChecker( Composition composition, ImmutableList<? extends SqlOperandTypeChecker> allowedRules, @Nullable String allowedSignatures, @Nullable SqlOperandCountRange range ) {
        this.allowedRules = Objects.requireNonNull( allowedRules );
        this.composition = Objects.requireNonNull( composition );
        this.allowedSignatures = allowedSignatures;
        this.range = range;
        assert (range != null) == (composition == Composition.REPEAT);
        assert allowedRules.size() + (range == null ? 0 : 1) > 1;
    }


    @Override
    public boolean isOptional( int i ) {
        for ( SqlOperandTypeChecker allowedRule : allowedRules ) {
            if ( allowedRule.isOptional( i ) ) {
                return true;
            }
        }
        return false;
    }


    public ImmutableList<? extends SqlOperandTypeChecker> getRules() {
        return allowedRules;
    }


    @Override
    public Consistency getConsistency() {
        return Consistency.NONE;
    }


    @Override
    public String getAllowedSignatures( SqlOperator op, String opName ) {
        if ( allowedSignatures != null ) {
            return allowedSignatures;
        }
        if ( composition == Composition.SEQUENCE ) {
            throw new AssertionError( "specify allowedSignatures or override getAllowedSignatures" );
        }
        StringBuilder ret = new StringBuilder();
        for ( Ord<SqlOperandTypeChecker> ord : Ord.<SqlOperandTypeChecker>zip( allowedRules ) ) {
            if ( ord.i > 0 ) {
                ret.append( SqlOperator.NL );
            }
            ret.append( ord.e.getAllowedSignatures( op, opName ) );
            if ( composition == Composition.AND ) {
                break;
            }
        }
        return ret.toString();
    }


    @Override
    public SqlOperandCountRange getOperandCountRange() {
        switch ( composition ) {
            case REPEAT:
                return range;
            case SEQUENCE:
                return SqlOperandCountRanges.of( allowedRules.size() );
            case AND:
            case OR:
            default:
                final List<SqlOperandCountRange> ranges =
                        new AbstractList<SqlOperandCountRange>() {
                            @Override
                            public SqlOperandCountRange get( int index ) {
                                return allowedRules.get( index ).getOperandCountRange();
                            }


                            @Override
                            public int size() {
                                return allowedRules.size();
                            }
                        };
                final int min = minMin( ranges );
                final int max = maxMax( ranges );
                SqlOperandCountRange composite =
                        new SqlOperandCountRange() {
                            @Override
                            public boolean isValidCount( int count ) {
                                switch ( composition ) {
                                    case AND:
                                        for ( SqlOperandCountRange range : ranges ) {
                                            if ( !range.isValidCount( count ) ) {
                                                return false;
                                            }
                                        }
                                        return true;
                                    case OR:
                                    default:
                                        for ( SqlOperandCountRange range : ranges ) {
                                            if ( range.isValidCount( count ) ) {
                                                return true;
                                            }
                                        }
                                        return false;
                                }
                            }


                            @Override
                            public int getMin() {
                                return min;
                            }


                            @Override
                            public int getMax() {
                                return max;
                            }
                        };
                if ( max >= 0 ) {
                    for ( int i = min; i <= max; i++ ) {
                        if ( !composite.isValidCount( i ) ) {
                            // Composite is not a simple range. Can't simplify, so return the composite.
                            return composite;
                        }
                    }
                }
                return min == max
                        ? SqlOperandCountRanges.of( min )
                        : SqlOperandCountRanges.between( min, max );
        }
    }


    private int minMin( List<SqlOperandCountRange> ranges ) {
        int min = Integer.MAX_VALUE;
        for ( SqlOperandCountRange range : ranges ) {
            min = Math.min( min, range.getMax() );
        }
        return min;
    }


    private int maxMax( List<SqlOperandCountRange> ranges ) {
        int max = Integer.MIN_VALUE;
        for ( SqlOperandCountRange range : ranges ) {
            if ( range.getMax() < 0 ) {
                if ( composition == Composition.OR ) {
                    return -1;
                }
            } else {
                max = Math.max( max, range.getMax() );
            }
        }
        return max;
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        if ( check( callBinding ) ) {
            return true;
        }
        if ( !throwOnFailure ) {
            return false;
        }
        if ( composition == Composition.OR ) {
            for ( SqlOperandTypeChecker allowedRule : allowedRules ) {
                allowedRule.checkOperandTypes( callBinding, true );
            }
        }

        // If no exception thrown, just throw a generic validation signature error.
        throw callBinding.newValidationSignatureError();
    }


    private boolean check( SqlCallBinding callBinding ) {
        switch ( composition ) {
            case REPEAT:
                if ( !range.isValidCount( callBinding.getOperandCount() ) ) {
                    return false;
                }
                for ( int operand : Util.range( callBinding.getOperandCount() ) ) {
                    for ( SqlOperandTypeChecker rule : allowedRules ) {
                        if ( !((SqlSingleOperandTypeChecker) rule).checkSingleOperandType(
                                callBinding,
                                callBinding.getCall().operand( operand ),
                                0,
                                false ) ) {
                            return false;
                        }
                    }
                }
                return true;

            case SEQUENCE:
                if ( callBinding.getOperandCount() != allowedRules.size() ) {
                    return false;
                }
                for ( Ord<SqlOperandTypeChecker> ord : Ord.<SqlOperandTypeChecker>zip( allowedRules ) ) {
                    SqlOperandTypeChecker rule = ord.e;
                    if ( !((SqlSingleOperandTypeChecker) rule).checkSingleOperandType(
                            callBinding,
                            callBinding.getCall().operand( ord.i ),
                            0,
                            false ) ) {
                        return false;
                    }
                }
                return true;

            case AND:
                for ( Ord<SqlOperandTypeChecker> ord : Ord.<SqlOperandTypeChecker>zip( allowedRules ) ) {
                    SqlOperandTypeChecker rule = ord.e;
                    if ( !rule.checkOperandTypes( callBinding, false ) ) {
                        // Avoid trying other rules in AND if the first one fails.
                        return false;
                    }
                }
                return true;

            case OR:
                for ( Ord<SqlOperandTypeChecker> ord : Ord.<SqlOperandTypeChecker>zip( allowedRules ) ) {
                    SqlOperandTypeChecker rule = ord.e;
                    if ( rule.checkOperandTypes( callBinding, false ) ) {
                        return true;
                    }
                }
                return false;

            default:
                throw new AssertionError();
        }
    }
}

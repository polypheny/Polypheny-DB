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

package org.polypheny.db.type.checker;


import com.google.common.collect.ImmutableList;
import java.util.AbstractList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.nodes.CallBinding;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.util.Util;


/**
 * This class allows multiple existing {@link PolyOperandTypeChecker} rules to be combined into one rule. For example,
 * allowing an operand to be either string or numeric could be done by:
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
 * For SEQUENCE composition, the rules must be instances of SqlSingleOperandTypeChecker, and signature generation is not
 * supported. For AND composition, only the first rule is used for signature generation.
 */
public class CompositeOperandTypeChecker implements PolyOperandTypeChecker {

    private final OperandCountRange range;


    /**
     * How operands are composed.
     */
    public enum Composition {
        AND, OR, SEQUENCE, REPEAT
    }


    protected final ImmutableList<? extends PolyOperandTypeChecker> allowedRules;
    protected final Composition composition;
    private final String allowedSignatures;


    /**
     * Package private. Use {@link OperandTypes#and}, {@link OperandTypes#or}.
     */
    CompositeOperandTypeChecker(
            Composition composition,
            ImmutableList<? extends PolyOperandTypeChecker> allowedRules,
            @Nullable String allowedSignatures,
            @Nullable OperandCountRange range ) {
        this.allowedRules = Objects.requireNonNull( allowedRules );
        this.composition = Objects.requireNonNull( composition );
        this.allowedSignatures = allowedSignatures;
        this.range = range;
        assert (range != null) == (composition == Composition.REPEAT);
        assert allowedRules.size() + (range == null ? 0 : 1) > 1;
    }


    @Override
    public boolean isOptional( int i ) {
        for ( PolyOperandTypeChecker allowedRule : allowedRules ) {
            if ( allowedRule.isOptional( i ) ) {
                return true;
            }
        }
        return false;
    }


    public ImmutableList<? extends PolyOperandTypeChecker> getRules() {
        return allowedRules;
    }


    @Override
    public Consistency getConsistency() {
        return Consistency.NONE;
    }


    @Override
    public String getAllowedSignatures( Operator op, String opName ) {
        if ( allowedSignatures != null ) {
            return allowedSignatures;
        }
        if ( composition == Composition.SEQUENCE ) {
            throw new AssertionError( "specify allowedSignatures or override getAllowedSignatures" );
        }
        StringBuilder ret = new StringBuilder();
        for ( Ord<PolyOperandTypeChecker> ord : Ord.<PolyOperandTypeChecker>zip( allowedRules ) ) {
            if ( ord.i > 0 ) {
                ret.append( Operator.NL );
            }
            ret.append( ord.e.getAllowedSignatures( op, opName ) );
            if ( composition == Composition.AND ) {
                break;
            }
        }
        return ret.toString();
    }


    @Override
    public OperandCountRange getOperandCountRange() {
        switch ( composition ) {
            case REPEAT:
                return range;
            case SEQUENCE:
                return PolyOperandCountRanges.of( allowedRules.size() );
            case AND:
            case OR:
            default:
                final List<OperandCountRange> ranges =
                        new AbstractList<>() {
                            @Override
                            public OperandCountRange get( int index ) {
                                return allowedRules.get( index ).getOperandCountRange();
                            }


                            @Override
                            public int size() {
                                return allowedRules.size();
                            }
                        };
                final int min = minMin( ranges );
                final int max = maxMax( ranges );
                OperandCountRange composite =
                        new OperandCountRange() {
                            @Override
                            public boolean isValidCount( int count ) {
                                return switch ( composition ) {
                                    case AND -> {
                                        for ( OperandCountRange range : ranges ) {
                                            if ( !range.isValidCount( count ) ) {
                                                yield false;
                                            }
                                        }
                                        yield true;
                                    }
                                    default -> {
                                        for ( OperandCountRange range : ranges ) {
                                            if ( range.isValidCount( count ) ) {
                                                yield true;
                                            }
                                        }
                                        yield false;
                                    }
                                };
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
                        ? PolyOperandCountRanges.of( min )
                        : PolyOperandCountRanges.between( min, max );
        }
    }


    private int minMin( List<OperandCountRange> ranges ) {
        int min = Integer.MAX_VALUE;
        for ( OperandCountRange range : ranges ) {
            min = Math.min( min, range.getMax() );
        }
        return min;
    }


    private int maxMax( List<OperandCountRange> ranges ) {
        int max = Integer.MIN_VALUE;
        for ( OperandCountRange range : ranges ) {
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
    public boolean checkOperandTypes( CallBinding callBinding, boolean throwOnFailure ) {
        if ( check( callBinding ) ) {
            return true;
        }
        if ( !throwOnFailure ) {
            return false;
        }
        if ( composition == Composition.OR ) {
            for ( PolyOperandTypeChecker allowedRule : allowedRules ) {
                allowedRule.checkOperandTypes( callBinding, true );
            }
        }

        // If no exception thrown, just throw a generic validation signature error.
        throw callBinding.newValidationSignatureError();
    }


    private boolean check( CallBinding callBinding ) {
        switch ( composition ) {
            case REPEAT:
                if ( !range.isValidCount( callBinding.getOperandCount() ) ) {
                    return false;
                }
                for ( int operand : Util.range( callBinding.getOperandCount() ) ) {
                    for ( PolyOperandTypeChecker rule : allowedRules ) {
                        if ( !((PolySingleOperandTypeChecker) rule).checkSingleOperandType(
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
                for ( Ord<PolyOperandTypeChecker> ord : Ord.<PolyOperandTypeChecker>zip( allowedRules ) ) {
                    PolyOperandTypeChecker rule = ord.e;
                    if ( !((PolySingleOperandTypeChecker) rule).checkSingleOperandType(
                            callBinding,
                            callBinding.getCall().operand( ord.i ),
                            0,
                            false ) ) {
                        return false;
                    }
                }
                return true;

            case AND:
                for ( Ord<PolyOperandTypeChecker> ord : Ord.<PolyOperandTypeChecker>zip( allowedRules ) ) {
                    PolyOperandTypeChecker rule = ord.e;
                    if ( !rule.checkOperandTypes( callBinding, false ) ) {
                        // Avoid trying other rules in AND if the first one fails.
                        return false;
                    }
                }
                return true;

            case OR:
                for ( Ord<PolyOperandTypeChecker> ord : Ord.<PolyOperandTypeChecker>zip( allowedRules ) ) {
                    PolyOperandTypeChecker rule = ord.e;
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

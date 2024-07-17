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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.rex;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Syntax;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.Litmus;


/**
 * An expression formed by a call to an operator with zero or more expressions as operands.
 *
 * Operators may be binary, unary, functions, special syntactic constructs like <code>CASE ... WHEN ... END</code>, or even internally generated constructs like implicit type conversions. The syntax of the operator is
 * really irrelevant, because row-expressions (unlike {@link Node SQL expressions}) do not directly represent a piece of source code.
 *
 * It's not often necessary to sub-class this class. The smarts should be in the operator, rather than the call. Any extra information about the call can often be encoded as extra arguments. (These don't need to be hidden,
 * because no one is going to be generating source code from this tree.)
 */
public class RexCall extends RexNode {

    public final Operator op;
    public final ImmutableList<RexNode> operands;
    public final AlgDataType type;

    public static final Set<Kind> SIMPLE_BINARY_OPS;


    static {
        EnumSet<Kind> kinds = EnumSet.of( Kind.PLUS, Kind.MINUS, Kind.TIMES, Kind.DIVIDE );
        kinds.addAll( Kind.COMPARISON );
        SIMPLE_BINARY_OPS = Sets.immutableEnumSet( kinds );
    }


    public RexCall( AlgDataType type, Operator op, RexNode... operands ) {
        this( type, op, ImmutableList.copyOf( operands ) );
    }

    public RexCall( AlgDataType type, Operator op, List<? extends RexNode> operands ) {
        this.type = Objects.requireNonNull( type );
        this.op = Objects.requireNonNull( op );
        this.operands = ImmutableList.copyOf( operands );
        assert op.getKind() != null : op;
        assert op.validRexOperands( operands.size(), Litmus.THROW ) : this;
    }


    /**
     * Appends call operands without parenthesis. {@link RexLiteral} might omit data type depending on the context.
     * For instance, {@code null:BOOLEAN} vs {@code =(true, null)}. The idea here is to omit "obvious" types for readability purposes while still maintain {@link AlgNode#getDigest()} contract.
     *
     * @param sb destination
     * @return original StringBuilder for fluent API
     * @see RexLiteral#computeDigest(RexDigestIncludeType)
     */
    protected final StringBuilder appendOperands( StringBuilder sb ) {
        for ( int i = 0; i < operands.size(); i++ ) {
            if ( i > 0 ) {
                sb.append( ", " );
            }
            RexNode operand = operands.get( i );
            if ( !(operand instanceof RexLiteral) ) {
                sb.append( operand );
                continue;
            }
            // Type information might be omitted in certain cases to improve readability
            // For instance, AND/OR arguments should be BOOLEAN, so AND(true, null) is better than AND(true, null:BOOLEAN), and we keep the same info +($0, 2) is better than +($0, 2:BIGINT). Note: if $0 has BIGINT,
            // then 2 is expected to be of BIGINT type as well.
            RexDigestIncludeType includeType = RexDigestIncludeType.OPTIONAL;
            if ( (isA( Kind.AND ) || isA( Kind.OR )) && operand.getType().getPolyType() == PolyType.BOOLEAN ) {
                includeType = RexDigestIncludeType.NO_TYPE;
            }
            if ( SIMPLE_BINARY_OPS.contains( getKind() ) ) {
                RexNode otherArg = operands.get( 1 - i );
                if ( (!(otherArg instanceof RexLiteral) || ((RexLiteral) otherArg).digestIncludesType() == RexDigestIncludeType.NO_TYPE) && equalSansNullability( operand.getType(), otherArg.getType() ) ) {
                    includeType = RexDigestIncludeType.NO_TYPE;
                }
            }
            sb.append( ((RexLiteral) operand).computeDigest( includeType ) );
        }
        return sb;
    }


    /**
     * This is a poorman's {@link PolyTypeUtil#equalSansNullability(AlgDataTypeFactory, AlgDataType, AlgDataType)}
     * {@code SqlTypeUtil} requires {@link AlgDataTypeFactory} which we haven't, so we assume that "not null" is represented in the type's digest as a trailing "NOT NULL" (case sensitive)
     *
     * @param a first type
     * @param b second type
     * @return true if the types are equal or the only difference is nullability
     */
    public static boolean equalSansNullability( AlgDataType a, AlgDataType b ) {
        String x = a.getFullTypeString();
        String y = b.getFullTypeString();
        if ( x.length() < y.length() ) {
            String c = x;
            x = y;
            y = c;
        }

        return (x.length() == y.length() || x.length() == y.length() + 9 && x.endsWith( " NOT NULL" )) && x.startsWith( y );
    }


    protected @Nonnull String computeDigest( boolean withType ) {
        final StringBuilder sb = new StringBuilder( op.getName() );
        if ( (operands.isEmpty()) && (op.getSyntax() == Syntax.FUNCTION_ID) ) {
            // Don't print params for empty arg list. For example, we want "SYSTEM_USER", not "SYSTEM_USER()".
        } else {
            sb.append( "(" );
            appendOperands( sb );
            sb.append( ")" );
        }
        if ( withType ) {
            sb.append( ":" );
            // NOTE jvs 16-Jan-2005:  for digests, it is very important to use the full type string.
            sb.append( type.getFullTypeString() );
        }
        return sb.toString();
    }


    @Override
    public final @Nonnull String toString() {
        // This data race is intentional
        String localDigest = digest;
        if ( localDigest == null ) {
            localDigest = computeDigest( isA( Kind.CAST ) || isA( Kind.NEW_SPECIFICATION ) );
            digest = Objects.requireNonNull( localDigest );
        }
        return localDigest;
    }


    @Override
    public <R> R accept( RexVisitor<R> visitor ) {
        return visitor.visitCall( this );
    }


    @Override
    public <R, P> R accept( RexBiVisitor<R, P> visitor, P arg ) {
        return visitor.visitCall( this, arg );
    }


    @Override
    public AlgDataType getType() {
        return type;
    }


    @Override
    public boolean isAlwaysTrue() {
        // "c IS NOT NULL" occurs when we expand EXISTS.
        // This reduction allows us to convert it to a semi-join.
        return switch ( getKind() ) {
            case IS_NOT_NULL -> !operands.get( 0 ).getType().isNullable();
            case IS_NOT_TRUE, IS_FALSE, NOT -> operands.get( 0 ).isAlwaysFalse();
            case IS_NOT_FALSE, IS_TRUE, CAST -> operands.get( 0 ).isAlwaysTrue();
            default -> false;
        };
    }


    @Override
    public boolean isAlwaysFalse() {
        return switch ( getKind() ) {
            case IS_NULL -> !operands.get( 0 ).getType().isNullable();
            case IS_NOT_TRUE, IS_FALSE, NOT -> operands.get( 0 ).isAlwaysTrue();
            case IS_NOT_FALSE, IS_TRUE, CAST -> operands.get( 0 ).isAlwaysFalse();
            default -> false;
        };
    }


    @Override
    public Kind getKind() {
        return op.getKind();
    }


    public List<RexNode> getOperands() {
        return operands;
    }


    public Operator getOperator() {
        return op;
    }


    /**
     * Creates a new call to the same operator with different operands.
     *
     * @param type Return type
     * @param operands Operands to call
     * @return New call
     */
    public RexCall clone( AlgDataType type, List<RexNode> operands ) {
        return new RexCall( type, op, operands );
    }


    @Override
    public boolean equals( Object obj ) {
        return obj == this
                || obj instanceof RexCall
                && toString().equals( obj.toString() );
    }


    @Override
    public int hashCode() {
        if ( Kind.MQL_KIND.contains( op.getKind() ) || OperatorName.MQL_OPERATORS.contains( this.op.getOperatorName() ) ) {
            return (op + "[" + operands.stream().map( rexNode -> Integer.toString( rexNode.hashCode() ) ).collect( Collectors.joining( "," ) ) + "]").hashCode();
        } else {
            return toString().hashCode();
        }
    }

}


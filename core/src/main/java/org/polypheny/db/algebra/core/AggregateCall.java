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

package org.polypheny.db.algebra.core;


import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.mapping.Mapping;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Call to an aggregate function within an {@link Aggregate}.
 */
public class AggregateCall {

    private final AggFunction aggFunction;

    private final boolean distinct;
    private final boolean approximate;
    public final AlgDataType type;
    @Getter
    public final String name;

    // We considered using ImmutableIntList but we would not save much memory: since all values are small,
    // ImmutableList uses cached Integer values.
    private final ImmutableList<Integer> argList;
    public final int filterArg;

    @Getter
    public final AlgCollation collation;


    /**
     * Creates an AggregateCall.
     *
     * @param aggFunction Aggregate function
     * @param distinct Whether distinct
     * @param approximate Whether approximate
     * @param argList List of ordinals of arguments
     * @param filterArg Ordinal of filter argument (the {@code FILTER (WHERE ...)} clause in SQL), or -1
     * @param collation How to sort values before aggregation (the {@code WITHIN GROUP} clause in SQL)
     * @param type Result type
     * @param name Name (may be null)
     */
    private AggregateCall(
            AggFunction aggFunction,
            boolean distinct,
            boolean approximate,
            List<Integer> argList,
            int filterArg,
            AlgCollation collation,
            AlgDataType type,
            String name ) {
        this.type = Objects.requireNonNull( type );
        this.name = name;
        this.aggFunction = Objects.requireNonNull( aggFunction );
        this.argList = ImmutableList.copyOf( argList );
        this.filterArg = filterArg;
        this.collation = Objects.requireNonNull( collation );
        this.distinct = distinct;
        this.approximate = approximate;
    }


    /**
     * Creates an AggregateCall, inferring its type if {@code type} is null.
     */
    public static AggregateCall create(
            AggFunction aggFunction,
            boolean distinct,
            boolean approximate,
            List<Integer> args,
            int filterArg,
            AlgCollation collation,
            int groupCount,
            AlgNode input,
            AlgDataType type,
            String name ) {
        if ( type == null ) {
            final AlgDataTypeFactory typeFactory = input.getCluster().getTypeFactory();
            final List<AlgDataType> types = PolyTypeUtil.projectTypes( input.getTupleType(), args );
            final Aggregate.AggCallBinding callBinding = new Aggregate.AggCallBinding(
                    typeFactory,
                    aggFunction,
                    types,
                    groupCount,
                    filterArg >= 0 );
            type = aggFunction.inferReturnType( callBinding );
        }
        return create( aggFunction, distinct, approximate, args, filterArg, collation, type, name );
    }


    /**
     * Creates an AggregateCall.
     */
    public static AggregateCall create(
            AggFunction aggFunction,
            boolean distinct,
            boolean approximate,
            List<Integer> args,
            int filterArg,
            AlgCollation collation,
            AlgDataType type,
            String name ) {
        return new AggregateCall( aggFunction, distinct, approximate, args, filterArg, collation, type, name );
    }


    /**
     * Returns whether this AggregateCall is distinct, as in <code>COUNT(DISTINCT empno)</code>.
     *
     * @return whether distinct
     */
    public final boolean isDistinct() {
        return distinct;
    }


    /**
     * Returns whether this AggregateCall is approximate, as in <code>APPROX_COUNT_DISTINCT(empno)</code>.
     *
     * @return whether approximate
     */
    public final boolean isApproximate() {
        return approximate;
    }


    /**
     * Returns the aggregate function.
     *
     * @return aggregate function
     */
    public final AggFunction getAggregation() {
        return aggFunction;
    }


    /**
     * Returns the ordinals of the arguments to this call.
     * <p>
     * The list is immutable.
     *
     * @return list of argument ordinals
     */
    public final List<Integer> getArgList() {
        return argList;
    }


    /**
     * Returns the result type.
     *
     * @return result type
     */
    public final AlgDataType getType() {
        return type;
    }


    /**
     * Creates an equivalent AggregateCall that has a new name.
     *
     * @param name New name (may be null)
     */
    public AggregateCall rename( String name ) {
        if ( Objects.equals( this.name, name ) ) {
            return this;
        }
        return new AggregateCall( aggFunction, distinct, approximate, argList, filterArg, AlgCollations.EMPTY, type, name );
    }


    public String toString() {
        return toString( null );
    }


    /**
     * If fieldNames is null, args are serialized to "$" + arg.
     * Otherwise, the value in fieldNames at index arg is used.
     *
     * @param fieldNames list containing the field names referenced by argList.
     * @return string representation of this instance.
     */
    public String toString( List<String> fieldNames ) {
        StringBuilder buf = new StringBuilder( aggFunction.toString() );
        buf.append( "(" );
        if ( distinct ) {
            buf.append( (argList.isEmpty()) ? "DISTINCT" : "DISTINCT " );
        }
        int i = -1;
        for ( Integer arg : argList ) {
            if ( ++i > 0 ) {
                buf.append( ", " );
            }
            if ( fieldNames == null ) {
                buf.append( "$" );
                buf.append( arg );
            } else {
                buf.append( fieldNames.get( arg ) );
            }
        }
        buf.append( ")" );
        if ( !collation.equals( AlgCollations.EMPTY ) ) {
            buf.append( " WITHIN GROUP (" );
            buf.append( collation );
            buf.append( ")" );
        }
        if ( hasFilter() ) {
            buf.append( " FILTER $" );
            buf.append( filterArg );
        }
        return buf.toString();
    }


    /**
     * Returns true if and only if this AggregateCall has a filter argument
     */
    public boolean hasFilter() {
        return filterArg >= 0;
    }


    @Override
    public boolean equals( Object o ) {
        if ( !(o instanceof AggregateCall other) ) {
            return false;
        }
        return aggFunction.equals( other.aggFunction )
                && (distinct == other.distinct)
                && argList.equals( other.argList )
                && filterArg == other.filterArg
                && Objects.equals( collation, other.collation );
    }


    @Override
    public int hashCode() {
        return Objects.hash( aggFunction, distinct, argList, filterArg, collation );
    }


    /**
     * Creates a binding of this call in the context of an {@link LogicalRelAggregate},
     * which can then be used to infer the return type.
     */
    public Aggregate.AggCallBinding createBinding( Aggregate aggregateRelBase ) {
        final AlgDataType rowType = aggregateRelBase.getInput().getTupleType();
        return new Aggregate.AggCallBinding(
                aggregateRelBase.getCluster().getTypeFactory(),
                aggFunction,
                PolyTypeUtil.projectTypes( rowType, argList ),
                aggregateRelBase.getGroupCount(),
                hasFilter() );
    }


    /**
     * Creates an equivalent AggregateCall with new argument ordinals.
     *
     * @param args Arguments
     * @return AggregateCall that suits new inputs and GROUP BY columns
     * @see #transform(Mappings.TargetMapping)
     */
    public AggregateCall copy( List<Integer> args, int filterArg, AlgCollation collation ) {
        return new AggregateCall( aggFunction, distinct, approximate, args, filterArg, collation, type, name );
    }


    /**
     * Creates equivalent AggregateCall that is adapted to a new input types and/or number of columns in GROUP BY.
     *
     * @param input relation that will be used as a child of aggregate
     * @param args argument indices of the new call in the input
     * @param filterArg Index of the filter, or -1
     * @param oldGroupKeyCount number of columns in GROUP BY of old aggregate
     * @param newGroupKeyCount number of columns in GROUP BY of new aggregate
     * @return AggregateCall that suits new inputs and GROUP BY columns
     */
    public AggregateCall adaptTo( AlgNode input, List<Integer> args, int filterArg, int oldGroupKeyCount, int newGroupKeyCount ) {
        // The return type of aggregate call need to be recomputed. Since it might depend on the number of columns in GROUP BY.
        final AlgDataType newType =
                oldGroupKeyCount == newGroupKeyCount
                        && args.equals( this.argList )
                        && filterArg == this.filterArg ? type : null;
        return create( aggFunction, distinct, approximate, args, filterArg, collation, newGroupKeyCount, input, newType, getName() );
    }


    /**
     * Creates a copy of this aggregate call, applying a mapping to its arguments.
     */
    public AggregateCall transform( Mappings.TargetMapping mapping ) {
        return copy(
                Mappings.apply2( (Mapping) mapping, argList ),
                hasFilter()
                        ? Mappings.apply( mapping, filterArg )
                        : -1,
                AlgCollations.permute( collation, mapping ) );
    }


    public AggregateCall adjustedCopy( List<Integer> args ) {
        return new AggregateCall( aggFunction, distinct, approximate, args, filterArg, collation, type, name );
    }

}

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


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.Set;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgFieldCollation.Direction;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.util.Pair;


/**
 * Expression combined with sort flags (DESCENDING, NULLS LAST).
 */
public class RexFieldCollation extends Pair<RexNode, ImmutableSet<Kind>> {

    /**
     * Canonical map of all combinations of {@link Kind} values that can ever occur.
     * We use a canonical map to save a bit of memory. Because the sets are EnumSets they have predictable order for toString().
     */
    private static final ImmutableMap<Set<Kind>, ImmutableSet<Kind>> KINDS =
            new Initializer()
                    .add()
                    .add( Kind.NULLS_FIRST )
                    .add( Kind.NULLS_LAST )
                    .add( Kind.DESCENDING )
                    .add( Kind.DESCENDING, Kind.NULLS_FIRST )
                    .add( Kind.DESCENDING, Kind.NULLS_LAST )
                    .build();


    public RexFieldCollation( RexNode left, Set<Kind> right ) {
        super( left, KINDS.get( right ) );
    }


    @Override
    public String toString() {
        return toString( null );
    }


    public String toString( RexVisitor<String> visitor ) {
        final String s = visitor == null ? left.toString() : left.accept( visitor );
        if ( right.isEmpty() ) {
            return s;
        }
        final StringBuilder b = new StringBuilder( s );
        for ( Kind operator : right ) {
            switch ( operator ) {
                case DESCENDING:
                    b.append( " DESC" );
                    break;
                case NULLS_FIRST:
                    b.append( " NULLS FIRST" );
                    break;
                case NULLS_LAST:
                    b.append( " NULLS LAST" );
                    break;
                default:
                    throw new AssertionError( operator );
            }
        }
        return b.toString();
    }


    public Direction getDirection() {
        return right.contains( Kind.DESCENDING )
                ? AlgFieldCollation.Direction.DESCENDING
                : AlgFieldCollation.Direction.ASCENDING;
    }


    public AlgFieldCollation.NullDirection getNullDirection() {
        return right.contains( Kind.NULLS_LAST )
                ? AlgFieldCollation.NullDirection.LAST
                : right.contains( Kind.NULLS_FIRST )
                        ? AlgFieldCollation.NullDirection.FIRST
                        : getDirection().defaultNullDirection();
    }


    /**
     * Helper, used during initialization, that builds a canonizing map from sets of {@code Kind} to immutable sets of {@code Kind}.
     */
    private static class Initializer {

        final ImmutableMap.Builder<Set<Kind>, ImmutableSet<Kind>> builder = ImmutableMap.builder();


        public Initializer add() {
            return add( ImmutableSet.of() );
        }


        public Initializer add( Kind kind, Kind... kinds ) {
            return add( Sets.immutableEnumSet( kind, kinds ) );
        }


        private Initializer add( ImmutableSet<Kind> set ) {
            builder.put( set, set );
            return this;
        }


        public ImmutableMap<Set<Kind>, ImmutableSet<Kind>> build() {
            return builder.build();
        }

    }

}


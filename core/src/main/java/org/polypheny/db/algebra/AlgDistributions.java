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

package org.polypheny.db.algebra;


import com.google.common.collect.Ordering;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.polypheny.db.plan.AlgMultipleTrait;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.runtime.ComparableList;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.mapping.Mapping;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Utilities concerning {@link AlgDistribution}.
 */
public class AlgDistributions {

    private static final ComparableList<Integer> EMPTY = ComparableList.of();

    /**
     * The singleton singleton distribution.
     */
    public static final AlgDistribution SINGLETON = new AlgDistributionImpl( AlgDistribution.Type.SINGLETON, EMPTY );

    /**
     * The singleton random distribution.
     */
    public static final AlgDistribution RANDOM_DISTRIBUTED = new AlgDistributionImpl( AlgDistribution.Type.RANDOM_DISTRIBUTED, EMPTY );

    /**
     * The singleton round-robin distribution.
     */
    public static final AlgDistribution ROUND_ROBIN_DISTRIBUTED = new AlgDistributionImpl( AlgDistribution.Type.ROUND_ROBIN_DISTRIBUTED, EMPTY );

    /**
     * The singleton broadcast distribution.
     */
    public static final AlgDistribution BROADCAST_DISTRIBUTED = new AlgDistributionImpl( AlgDistribution.Type.BROADCAST_DISTRIBUTED, EMPTY );

    public static final AlgDistribution ANY = new AlgDistributionImpl( AlgDistribution.Type.ANY, EMPTY );


    private AlgDistributions() {
    }


    /**
     * Creates a hash distribution.
     */
    public static AlgDistribution hash( Collection<Integer> numbers ) {
        ComparableList<Integer> list = ComparableList.copyOf( numbers );
        if ( numbers.size() > 1 && !Ordering.natural().isOrdered( list ) ) {
            list = ComparableList.copyOf( Ordering.natural().sortedCopy( list ) );
        }
        AlgDistributionImpl trait = new AlgDistributionImpl( AlgDistribution.Type.HASH_DISTRIBUTED, list );
        return AlgDistributionTraitDef.INSTANCE.canonize( trait );
    }


    /**
     * Creates a range distribution.
     */
    public static AlgDistribution range( Collection<Integer> numbers ) {
        ComparableList<Integer> list = ComparableList.copyOf( numbers );
        AlgDistributionImpl trait = new AlgDistributionImpl( AlgDistribution.Type.RANGE_DISTRIBUTED, list );
        return AlgDistributionTraitDef.INSTANCE.canonize( trait );
    }


    /**
     * Implementation of {@link AlgDistribution}.
     */
    private static class AlgDistributionImpl implements AlgDistribution {

        private static final Ordering<Iterable<Integer>> ORDERING = Ordering.<Integer>natural().lexicographical();
        private final Type type;
        private final ComparableList<Integer> keys;


        private AlgDistributionImpl( Type type, ComparableList<Integer> keys ) {
            this.type = Objects.requireNonNull( type );
            this.keys = ComparableList.copyOf( keys );
            assert type != Type.HASH_DISTRIBUTED || keys.size() < 2 || Ordering.natural().isOrdered( keys ) : "key columns of hash distribution must be in order";
            assert type == Type.HASH_DISTRIBUTED || type == Type.RANDOM_DISTRIBUTED || keys.isEmpty();
        }


        @Override
        public int hashCode() {
            return Objects.hash( type, keys );
        }


        @Override
        public boolean equals( Object obj ) {
            return this == obj
                    || obj instanceof AlgDistributionImpl
                    && type == ((AlgDistributionImpl) obj).type
                    && keys.equals( ((AlgDistributionImpl) obj).keys );
        }


        @Override
        public String toString() {
            if ( keys.isEmpty() ) {
                return type.shortName;
            } else {
                return type.shortName + keys;
            }
        }


        @Override
        @Nonnull
        public Type getType() {
            return type;
        }


        @Override
        @Nonnull
        public List<Integer> getKeys() {
            return keys;
        }


        @Override
        public AlgDistributionTraitDef getTraitDef() {
            return AlgDistributionTraitDef.INSTANCE;
        }


        @Override
        public AlgDistribution apply( Mappings.TargetMapping mapping ) {
            if ( keys.isEmpty() ) {
                return this;
            }
            return getTraitDef().canonize( new AlgDistributionImpl( type, ComparableList.copyOf( Mappings.apply( (Mapping) mapping, keys ) ) ) );
        }


        @Override
        public boolean satisfies( AlgTrait<?> trait ) {
            if ( trait == this || trait == ANY ) {
                return true;
            }
            if ( trait instanceof AlgDistributionImpl ) {
                AlgDistributionImpl distribution = (AlgDistributionImpl) trait;
                if ( type == distribution.type ) {
                    switch ( type ) {
                        case HASH_DISTRIBUTED:
                            // The "leading edge" property of Range does not apply to Hash. Only Hash[x, y] satisfies Hash[x, y].
                            return keys.equals( distribution.keys );
                        case RANGE_DISTRIBUTED:
                            // Range[x, y] satisfies Range[x, y, z] but not Range[x]
                            return Util.startsWith( distribution.keys, keys );
                        default:
                            return true;
                    }
                }
            }
            if ( trait == RANDOM_DISTRIBUTED ) {
                // RANDOM is satisfied by HASH, ROUND-ROBIN, RANDOM, RANGE; we've already checked RANDOM
                return type == Type.HASH_DISTRIBUTED
                        || type == Type.ROUND_ROBIN_DISTRIBUTED
                        || type == Type.RANGE_DISTRIBUTED;
            }
            return false;
        }


        @Override
        public void register( AlgPlanner planner ) {
        }


        @Override
        public boolean isTop() {
            return type == Type.ANY;
        }


        @Override
        public int compareTo( @Nonnull AlgMultipleTrait o ) {
            final AlgDistribution distribution = (AlgDistribution) o;
            if ( type == distribution.getType()
                    && (type == Type.HASH_DISTRIBUTED
                    || type == Type.RANGE_DISTRIBUTED) ) {
                return ORDERING.compare( getKeys(), distribution.getKeys() );
            }

            return type.compareTo( distribution.getType() );
        }

    }

}


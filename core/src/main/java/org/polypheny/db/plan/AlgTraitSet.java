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

package org.polypheny.db.plan;


import com.google.common.collect.ImmutableList;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.schema.trait.ModelTraitDef;
import org.polypheny.db.util.Pair;


/**
 * RelTraitSet represents an ordered set of {@link AlgTrait}s.
 */
public final class AlgTraitSet extends AbstractList<AlgTrait<?>> {

    private static final AlgTrait<?>[] EMPTY_TRAITS = new AlgTrait[0];

    private final Cache cache;
    private final AlgTrait<? extends AlgTraitDef<?>>[] traits;
    private final String string;


    /**
     * Constructs a RelTraitSet with the given set of RelTraits.
     *
     * @param cache Trait set cache (and indirectly cluster) that this set belongs to
     * @param traits Traits
     */
    private AlgTraitSet( Cache cache, AlgTrait<?>[] traits ) {
        // NOTE: We do not copy the array. It is important that the array is not shared. However, since this constructor is private, we assume that the caller has made a copy.
        this.cache = cache;
        this.traits = traits;
        this.string = computeString();
    }


    /**
     * Creates an empty trait set.
     *
     * It has a new cache, which will be shared by any trait set created from it. Thus each empty trait set is the start of a new ancestral line.
     */
    public static AlgTraitSet createEmpty() {
        return new AlgTraitSet( new Cache(), EMPTY_TRAITS );
    }


    /**
     * Retrieves a RelTrait from the set.
     *
     * @param index 0-based index into ordered RelTraitSet
     * @return the RelTrait
     * @throws ArrayIndexOutOfBoundsException if index greater than or equal to {@link #size()} or less than 0.
     */
    public AlgTrait<?> getTrait( int index ) {
        return traits[index];
    }


    /**
     * Retrieves a list of traits from the set.
     *
     * @param index 0-based index into ordered RelTraitSet
     * @return the RelTrait
     * @throws ArrayIndexOutOfBoundsException if index greater than or equal to {@link #size()} or less than 0.
     */
    public <E extends AlgMultipleTrait<?>> List<E> getTraits( int index ) {
        final AlgTrait<?> trait = traits[index];
        if ( trait instanceof AlgCompositeTrait ) {
            return ((AlgCompositeTrait<E>) trait).traitList();
        } else {
            //noinspection unchecked
            return ImmutableList.of( (E) trait );
        }
    }


    @Override
    public AlgTrait<?> get( int index ) {
        return getTrait( index );
    }


    /**
     * Returns whether a given kind of trait is enabled.
     */
    public <T extends AlgTrait<?>> boolean isEnabled( AlgTraitDef<T> traitDef ) {
        return getTrait( traitDef ) != null;
    }


    /**
     * Retrieves a RelTrait of the given type from the set.
     *
     * @param traitDef the type of RelTrait to retrieve
     * @return the RelTrait, or null if not found
     */
    public <T extends AlgTrait<?>> T getTrait( AlgTraitDef<T> traitDef ) {
        int index = findIndex( traitDef );
        if ( index >= 0 ) {
            //noinspection unchecked
            return (T) getTrait( index );
        }
        if ( traitDef == ModelTraitDef.INSTANCE ) {
            return (T) ModelTrait.RELATIONAL;
        }

        return null;
    }


    /**
     * Retrieves a list of traits of the given type from the set.
     *
     * Only valid for traits that support multiple entries. (E.g. collation.)
     *
     * @param traitDef the type of RelTrait to retrieve
     * @return the RelTrait, or null if not found
     */
    public <T extends AlgMultipleTrait<?>> List<T> getTraits( AlgTraitDef<T> traitDef ) {
        int index = findIndex( traitDef );
        if ( index >= 0 ) {
            return getTraits( index );
        }

        return null;
    }


    /**
     * Replaces an existing RelTrait in the set. Returns a different trait set; does not modify this trait set.
     *
     * @param index 0-based index into ordered RelTraitSet
     * @param trait the new RelTrait
     * @return the old RelTrait at the index
     */
    public AlgTraitSet replace( int index, AlgTrait<?> trait ) {
        assert traits[index].getTraitDef() == trait.getTraitDef() : "AlgTrait has different RelTraitDef than replacement";

        AlgTrait<?> canonizedTrait = canonize( trait );
        if ( traits[index] == canonizedTrait ) {
            return this;
        }
        AlgTrait<?>[] newTraits = traits.clone();
        newTraits[index] = canonizedTrait;
        return cache.getOrAdd( new AlgTraitSet( cache, newTraits ) );
    }


    /**
     * Returns a trait set consisting of the current set plus a new trait.
     *
     * If the set does not contain a trait of the same {@link AlgTraitDef}, the trait is ignored, and this trait set is returned.
     *
     * @param trait the new trait
     * @return New set
     * @see #plus(AlgTrait)
     */
    public AlgTraitSet replace( AlgTrait<?> trait ) {
        // Quick check for common case
        if ( containsShallow( traits, trait ) ) {
            return this;
        }
        final AlgTraitDef<?> traitDef = trait.getTraitDef();
        int index = findIndex( traitDef );
        if ( index < 0 ) {
            // Trait is not present. Ignore it.
            return this;
        }

        return replace( index, trait );
    }


    /**
     * Returns whether an element occurs within an array.
     *
     * Uses {@code ==}, not {@link #equals}. Nulls are allowed.
     */
    private static <T> boolean containsShallow( T[] ts, AlgTrait<?> seek ) {
        for ( T t : ts ) {
            if ( t == seek ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Replaces the trait(s) of a given type with a list of traits of the same type.
     *
     * The list must not be empty, and all traits must be of the same type.
     */
    public <T extends AlgMultipleTrait<?>> AlgTraitSet replace( List<T> traits ) {
        assert !traits.isEmpty();
        final AlgTraitDef<?> def = traits.get( 0 ).getTraitDef();
        return replace( AlgCompositeTrait.of( def, traits ) );
    }


    /**
     * Replaces the trait(s) of a given type with a list of traits of the same type.
     *
     * The list must not be empty, and all traits must be of the same type.
     */
    public <T extends AlgMultipleTrait<?>> AlgTraitSet replace( AlgTraitDef<T> def, List<T> traits ) {
        return replace( AlgCompositeTrait.of( def, traits ) );
    }


    /**
     * If a given multiple trait is enabled, replaces it by calling the given function.
     */
    public <T extends AlgMultipleTrait<?>> AlgTraitSet replaceIfs( AlgTraitDef<T> def, Supplier<List<T>> traitSupplier ) {
        int index = findIndex( def );
        if ( index < 0 ) {
            return this; // trait is not enabled; ignore it
        }
        final List<T> traitList = traitSupplier.get();
        return replace( index, AlgCompositeTrait.of( def, traitList ) );
    }


    /**
     * If a given trait is enabled, replaces it by calling the given function.
     */
    public <T extends AlgTrait<?>> AlgTraitSet replaceIf( AlgTraitDef<T> def, Supplier<T> traitSupplier ) {
        int index = findIndex( def );
        if ( index < 0 ) {
            return this; // trait is not enabled; ignore it
        }
        final T traitList = traitSupplier.get();
        return replace( index, traitList );
    }


    /**
     * Returns the size of the RelTraitSet.
     *
     * @return the size of the RelTraitSet.
     */
    @Override
    public int size() {
        return traits.length;
    }


    /**
     * Converts a trait to canonical form.
     *
     * After canonization, t1.equals(t2) if and only if t1 == t2.
     *
     * @param trait Trait
     * @return Trait in canonical form
     */
    public <T extends AlgTrait> T canonize( T trait ) {
        if ( trait == null ) {
            return null;
        }

        if ( trait instanceof AlgCompositeTrait ) {
            // Composite traits are canonized on creation
            return trait;
        }

        //noinspection unchecked
        return (T) trait.getTraitDef().canonize( trait );
    }


    /**
     * Compares two RelTraitSet objects for equality.
     *
     * @param obj another RelTraitSet
     * @return true if traits are equal and in the same order, false otherwise
     */
    @Override
    public boolean equals( Object obj ) {
        return this == obj
                || obj instanceof AlgTraitSet
                && Arrays.equals( traits, ((AlgTraitSet) obj).traits );
    }


    @Override
    public int hashCode() {
        return Arrays.hashCode( traits );
    }


    /**
     * Returns whether this trait set satisfies another trait set.
     *
     * For that to happen, each trait satisfies the corresponding trait in the other set. In particular, each trait set satisfies itself, because each trait subsumes itself.
     *
     * Intuitively, if a relational expression is needed that has trait set S (A, B), and trait set S1 (A1, B1) subsumes S, then any relational expression R in S1 meets that need.
     *
     * For example, if we need a relational expression that has trait set S = {enumerable convention, sorted on [C1 asc]}, and R has {enumerable convention, sorted on [C3], [C1, C2]}.
     * R has two sort keys, but one them [C1, C2] satisfies S [C1], and that is enough.
     *
     * @param that another RelTraitSet
     * @return whether this trait set satisfies other trait set
     * @see AlgTrait#satisfies(AlgTrait)
     */
    public boolean satisfies( AlgTraitSet that ) {
        for ( Pair<AlgTrait<?>, AlgTrait<?>> pair : Pair.zip( traits, that.traits ) ) {
            if ( !pair.left.satisfies( pair.right ) ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Compares two RelTraitSet objects to see if they match for the purposes of firing a rule. A null RelTrait within a RelTraitSet indicates a wildcard: any RelTrait in the other
     * RelTraitSet will match. If one RelTraitSet is smaller than the other, comparison stops when the last RelTrait from the smaller set has been examined and the remaining RelTraits in the larger
     * set are assumed to match.
     *
     * @param that another RelTraitSet
     * @return true if the RelTraitSets match, false otherwise
     */
    public boolean matches( AlgTraitSet that ) {
        final int n = Math.min( this.size(), that.size() );

        for ( int i = 0; i < n; i++ ) {
            AlgTrait<?> thisTrait = this.traits[i];
            AlgTrait<?> thatTrait = that.traits[i];

            if ( (thisTrait == null) || (thatTrait == null) ) {
                continue;
            }

            if ( thisTrait != thatTrait ) {
                return false;
            }
        }

        return true;
    }


    /**
     * Returns whether this trait set contains a given trait.
     *
     * @param trait Sought trait
     * @return Whether set contains given trait
     */
    public boolean contains( AlgTrait<?> trait ) {
        for ( AlgTrait<?> algTrait : traits ) {
            if ( trait == algTrait ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Returns whether this trait set contains the given trait, or whether the trait is not present because its {@link AlgTraitDef} is not enabled.
     * Returns false if another trait of the same {@code RelTraitDef} is present.
     *
     * @param trait Trait
     * @return Whether trait is present, or is absent because disabled
     */
    public boolean containsIfApplicable( AlgTrait<?> trait ) {
        // Note that '==' is sufficient, because trait should be canonized.
        final AlgTrait<?> trait1 = getTrait( trait.getTraitDef() );
        return trait1 == null || trait1.equals( trait );
    }


    /**
     * Returns whether this trait set comprises precisely the list of given traits.
     *
     * @param algTraits Traits
     * @return Whether this trait set's traits are the same as the argument
     */
    public boolean comprises( AlgTrait<?>... algTraits ) {
        return Arrays.equals( traits, algTraits );
    }


    @Override
    public String toString() {
        return string;
    }


    /**
     * Outputs the traits of this set as a String. Traits are output in order, separated by periods.
     */
    private String computeString() {
        StringBuilder s = new StringBuilder();
        for ( int i = 0; i < traits.length; i++ ) {
            final AlgTrait<?> trait = traits[i];
            if ( i > 0 ) {
                s.append( '.' );
            }
            if ( (trait == null) && (traits.length == 1) ) {
                // Special format for a list containing a single null trait; otherwise its string appears as "null", which is the same as if the whole trait set were null, and so confusing.
                s.append( "{null}" );
            } else {
                s.append( trait );
            }
        }
        return s.toString();
    }


    /**
     * Finds the index of a trait of a given type in this set.
     *
     * @param traitDef Sought trait definition
     * @return index of trait, or -1 if not found
     */
    private int findIndex( AlgTraitDef<?> traitDef ) {
        for ( int i = 0; i < traits.length; i++ ) {
            AlgTrait<?> trait = traits[i];
            if ( (trait != null) && (trait.getTraitDef() == traitDef) ) {
                return i;
            }
        }

        return -1;
    }


    /**
     * Returns this trait set with a given trait added or overridden. Does not modify this trait set.
     *
     * @param trait Trait
     * @return Trait set with given trait
     */
    public AlgTraitSet plus( AlgTrait<?> trait ) {
        if ( contains( trait ) ) {
            return this;
        }
        int i = findIndex( trait.getTraitDef() );
        if ( i >= 0 ) {
            return replace( i, trait );
        }
        // Optimize time & space to represent a trait set key.
        //
        // Don't build a trait set until we're sure there isn't an equivalent one. Then we can justify the cost of computing RelTraitSet.string in the constructor.
        final AlgTrait<?> canonizedTrait = canonize( trait );
        assert canonizedTrait != null;
        List<AlgTrait<?>> newTraits;
        switch ( traits.length ) {
            case 0:
                newTraits = ImmutableList.of( canonizedTrait );
                break;
            case 1:
                newTraits = List.of( traits[0], canonizedTrait );
                break;
            case 2:
                newTraits = List.of( traits[0], traits[1], canonizedTrait );
                break;
            default:
                newTraits = ImmutableList.<AlgTrait<?>>builder().add( traits ).add( canonizedTrait ).build();
        }
        return cache.getOrAdd( newTraits );
    }


    public AlgTraitSet plusAll( AlgTrait<?>[] traits ) {
        AlgTraitSet t = this;
        for ( AlgTrait<?> trait : traits ) {
            t = t.plus( trait );
        }
        return t;
    }


    public AlgTraitSet merge( AlgTraitSet additionalTraits ) {
        return plusAll( additionalTraits.traits );
    }


    /**
     * Returns a list of traits that are in {@code traitSet} but not in this RelTraitSet.
     */
    public ImmutableList<AlgTrait<?>> difference( AlgTraitSet traitSet ) {
        final ImmutableList.Builder<AlgTrait<?>> builder = ImmutableList.builder();
        for ( Pair<AlgTrait<?>, AlgTrait<?>> pair : Pair.zip( traits, traitSet.traits ) ) {
            if ( pair.left != pair.right ) {
                builder.add( pair.right );
            }
        }
        return builder.build();
    }


    /**
     * Returns whether there are any composite traits in this set.
     */
    public boolean allSimple() {
        for ( AlgTrait<?> trait : traits ) {
            if ( trait instanceof AlgCompositeTrait ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Returns a trait set similar to this one but with all composite traits
     * flattened.
     */
    public AlgTraitSet simplify() {
        AlgTraitSet x = this;
        for ( int i = 0; i < traits.length; i++ ) {
            final AlgTrait<?> trait = traits[i];
            if ( trait instanceof AlgCompositeTrait ) {
                x = x.replace(
                        i,
                        ((AlgCompositeTrait<?>) trait).size() == 1
                                ? ((AlgCompositeTrait<?>) trait).trait( 0 )
                                : trait.getTraitDef().getDefault() );
            }
        }
        return x;
    }


    /**
     * Cache of trait sets.
     */
    private static class Cache {

        final Map<List<AlgTrait<?>>, AlgTraitSet> map = new HashMap<>();


        Cache() {
        }


        AlgTraitSet getOrAdd( List<AlgTrait<?>> traits ) {
            AlgTraitSet traitSet1 = map.get( traits );
            if ( traitSet1 != null ) {
                return traitSet1;
            }
            final AlgTraitSet traitSet = new AlgTraitSet( this, traits.toArray( new AlgTrait[0] ) );
            map.put( traits, traitSet );
            return traitSet;
        }

    }

}


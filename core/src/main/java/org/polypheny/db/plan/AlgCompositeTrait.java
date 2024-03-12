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
import com.google.common.collect.Ordering;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;


/**
 * A trait that consists of a list of traits, all of the same type.
 *
 * It exists so that multiple traits of the same type ({@link AlgTraitDef}) can be stored in the same {@link AlgTraitSet}.
 *
 * @param <T> Member trait
 */
class AlgCompositeTrait<T extends AlgMultipleTrait<?> & AlgTrait<?>> implements AlgTrait {

    private final AlgTraitDef<T> traitDef;
    private final T[] traits;


    /**
     * Creates a RelCompositeTrait.
     */
    // Must remain private. Does not copy the array.
    private AlgCompositeTrait( AlgTraitDef<T> traitDef, T[] traits ) {
        this.traitDef = traitDef;
        this.traits = Objects.requireNonNull( traits );
        assert Ordering.natural().isStrictlyOrdered( Arrays.asList( (Comparable<?>[]) traits ) ) : Arrays.toString( traits );
        for ( T trait : traits ) {
            assert trait.getTraitDef() == this.traitDef;
        }
    }


    /**
     * Creates a RelCompositeTrait. The constituent traits are canonized.
     */
    @SuppressWarnings("unchecked")
    static <T extends AlgMultipleTrait<?>> AlgTrait<?> of( AlgTraitDef def, List<T> traitList ) {
        final AlgCompositeTrait<T> compositeTrait;
        if ( traitList.isEmpty() ) {
            return def.getDefault();
        } else if ( traitList.size() == 1 ) {
            return def.canonize( traitList.get( 0 ) );
        } else {
            final AlgMultipleTrait<?>[] traits = traitList.toArray( new AlgMultipleTrait[0] );
            for ( int i = 0; i < traits.length; i++ ) {
                traits[i] = (T) def.canonize( traits[i] );
            }
            compositeTrait = new AlgCompositeTrait<>( def, (T[]) traits );
        }
        return def.canonize( compositeTrait );
    }


    @Override
    public AlgTraitDef<?> getTraitDef() {
        return traitDef;
    }


    @Override
    public int hashCode() {
        return Arrays.hashCode( traits );
    }


    @Override
    public boolean equals( Object obj ) {
        return this == obj
                || obj instanceof AlgCompositeTrait
                && Arrays.equals( traits, ((AlgCompositeTrait<?>) obj).traits );
    }


    @Override
    public String toString() {
        return Arrays.toString( traits );
    }


    @Override
    public boolean satisfies( AlgTrait trait ) {
        for ( T t : traits ) {
            if ( t.satisfies( trait ) ) {
                return true;
            }
        }
        return false;
    }


    @Override
    public void register( AlgPlanner planner ) {
    }


    /**
     * Returns an immutable list of the traits in this composite trait.
     */
    public List<T> traitList() {
        return ImmutableList.copyOf( traits );
    }


    /**
     * Returns the {@code i}th trait.
     */
    public T trait( int i ) {
        return traits[i];
    }


    /**
     * Returns the number of traits.
     */
    public int size() {
        return traits.length;
    }

}


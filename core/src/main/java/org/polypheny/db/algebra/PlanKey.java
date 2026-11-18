/*
 * Copyright 2019-2026 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.algebra;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.polypheny.db.algebra.PlanKeyComponents.Component;

/** Immutable structural identity of a plan or a part of a plan. */
public final class PlanKey {

    private final List<Object> components;
    private final int hash;


    private PlanKey( List<Object> components ) {
        this.components = components;
        this.hash = components.hashCode();
    }


    /** Returns an empty result if a component cannot be represented safely. */
    public static Optional<PlanKey> of( Object... components ) {
        List<Object> snapshot = PlanKeyComponents.snapshot( components == null ? Collections.singletonList( null ) : Arrays.asList( components ) );
        return snapshot == null ? Optional.empty() : Optional.of( new PlanKey( snapshot ) );
    }


    @Override
    public boolean equals( Object other ) {
        if ( this == other ) {
            return true;
        }
        if ( !(other instanceof PlanKey key) || hash != key.hash ) {
            return false;
        }

        // A plan or literal value can have shared inputs. Visit each pair of substructures once.
        Deque<Comparison> pending = new ArrayDeque<>();
        Map<Object, Set<Object>> compared = new IdentityHashMap<>();
        pending.push( new Comparison( this, key ) );
        while ( !pending.isEmpty() ) {
            Comparison pair = pending.pop();
            if ( pair.first == pair.second ) {
                continue;
            }

            List<Object> firstItems;
            List<Object> secondItems;
            if ( pair.first instanceof PlanKey first ) {
                if ( !(pair.second instanceof PlanKey second) || first.hash != second.hash ) {
                    return false;
                }
                firstItems = first.components;
                secondItems = second.components;
            } else if ( pair.first instanceof Component first ) {
                if ( !(pair.second instanceof Component second) || first.type() != second.type() || first.hash() != second.hash() ) {
                    return false;
                }
                firstItems = first.attributes();
                secondItems = second.attributes();
            } else {
                if ( !pair.first.equals( pair.second ) ) {
                    return false;
                }
                continue;
            }

            if ( firstItems.size() != secondItems.size() ) {
                return false;
            }
            if ( compared.computeIfAbsent( pair.first, ignored -> Collections.newSetFromMap( new IdentityHashMap<>( 1 ) ) ).add( pair.second ) ) {
                for ( int i = firstItems.size() - 1; i >= 0; i-- ) {
                    pending.push( new Comparison( firstItems.get( i ), secondItems.get( i ) ) );
                }
            }
        }
        return true;
    }


    @Override
    public int hashCode() {
        return hash;
    }


    private record Comparison( Object first, Object second ) {
    }

}

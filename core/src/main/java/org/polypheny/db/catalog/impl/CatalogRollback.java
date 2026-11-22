/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.catalog.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;


/**
 * In-memory rollback state for an exclusive catalog writer. Entries are immutable
 * unless a catalog supplies an explicit copy function.
 */
public final class CatalogRollback {

    private final List<Runnable> restoreActions = new ArrayList<>();


    public <K, V> void capture( Map<K, V> entries ) {
        capture( entries, UnaryOperator.identity() );
    }


    public <K, V> void capture( Map<K, V> entries, UnaryOperator<V> copyValue ) {
        Map<K, V> previous = new HashMap<>();
        entries.forEach( ( key, value ) -> previous.put( key, copyValue.apply( value ) ) );
        add( () -> {
            entries.keySet().retainAll( previous.keySet() );
            entries.putAll( previous );
        } );
    }


    public <E> void capture( Set<E> entries ) {
        Set<E> previous = new HashSet<>( entries );
        add( () -> {
            entries.retainAll( previous );
            entries.addAll( previous );
        } );
    }


    public void add( Runnable restoreAction ) {
        restoreActions.add( restoreAction );
    }


    public void restore() {
        restoreActions.forEach( Runnable::run );
    }

}

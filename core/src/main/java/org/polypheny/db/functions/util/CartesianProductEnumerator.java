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
 */

package org.polypheny.db.functions.util;

import java.util.List;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.type.entity.PolyValue;

public abstract class CartesianProductEnumerator<T extends PolyValue, E extends PolyValue> implements Enumerator<E> {

    private final List<Enumerator<T>> enumerators;
    protected final PolyValue[] elements;
    private boolean first = true;


    protected CartesianProductEnumerator( List<Enumerator<T>> enumerators ) {
        this.enumerators = enumerators;
        this.elements = new PolyValue[enumerators.size()];
    }


    @Override
    public boolean moveNext() {
        if ( first ) {
            int i = 0;
            for ( Enumerator<T> enumerator : enumerators ) {
                if ( !enumerator.moveNext() ) {
                    return false;
                }
                elements[i++] = enumerator.current();
            }
            first = false;
            return true;
        }
        for ( int ordinal = enumerators.size() - 1; ordinal >= 0; --ordinal ) {
            final Enumerator<T> enumerator = enumerators.get( ordinal );
            if ( enumerator.moveNext() ) {
                elements[ordinal] = enumerator.current();
                return true;
            }

            // Move back to first element.
            enumerator.reset();
            if ( !enumerator.moveNext() ) {
                // Very strange... this was empty all along.
                return false;
            }
            elements[ordinal] = enumerator.current();
        }
        return false;
    }


    @Override
    public void reset() {
        first = true;
        for ( Enumerator<T> enumerator : enumerators ) {
            enumerator.reset();
        }
    }


    @Override
    public void close() {
        // If there is one or more exceptions, carry on and close all enumerators,
        // then throw the first.
        Throwable rte = null;
        for ( Enumerator<T> enumerator : enumerators ) {
            try {
                enumerator.close();
            } catch ( Throwable e ) {
                if ( rte == null ) {
                    rte = e;
                } else {
                    rte.addSuppressed( e );
                }
            }
        }
        if ( rte != null ) {
            if ( rte instanceof Error ) {
                throw (Error) rte;
            } else {
                throw (RuntimeException) rte;
            }
        }
    }

}

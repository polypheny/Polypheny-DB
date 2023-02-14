/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.util;

import com.google.common.collect.Ordering;
import org.polypheny.db.algebra.constant.MonikerType;

import java.util.Comparator;
import java.util.List;


/**
 * An interface of an object identifier that represents a SqlIdentifier
 */
public interface Moniker {

    Comparator<Moniker> COMPARATOR =
            new Comparator<>() {
                final Ordering<Iterable<String>> listOrdering = Ordering.<String>natural().lexicographical();


                @Override
                public int compare( Moniker o1, Moniker o2 ) {
                    int c = o1.getType().compareTo( o2.getType() );
                    if ( c == 0 ) {
                        c = listOrdering.compare( o1.getFullyQualifiedNames(), o2.getFullyQualifiedNames() );
                    }
                    return c;
                }
            };

    /**
     * Returns the type of object referred to by this moniker. Never null.
     */
    MonikerType getType();

    /**
     * Returns the array of component names.
     */
    List<String> getFullyQualifiedNames();

    String id();

}


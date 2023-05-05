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

package org.polypheny.db.adapter.enumerable.enumerable;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;

public class SimpleEnumerable<E> extends AbstractEnumerable<E> {

    private final Enumerator<E> enumerator;

    private E current;


    public SimpleEnumerable( Enumerator<E> enumerator ) {
        this.enumerator = enumerator;
    }


    @Override
    public Enumerator<E> enumerator() {
        return enumerator;
    }


}

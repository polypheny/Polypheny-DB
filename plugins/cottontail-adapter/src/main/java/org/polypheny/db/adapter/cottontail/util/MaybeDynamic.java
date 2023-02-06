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

package org.polypheny.db.adapter.cottontail.util;


public class MaybeDynamic<T> {

    private final boolean dynamic;
    private final Long index;
    private final T value;


    private MaybeDynamic( boolean dynamic, Long index, T value ) {
        this.dynamic = dynamic;
        this.index = index;
        this.value = value;
    }


    public static <T> MaybeDynamic<T> dynamic( Long index ) {
        return new MaybeDynamic<>( true, index, null );
    }


    public static <T> MaybeDynamic<T> fixed( T value ) {
        return new MaybeDynamic<>( false, 0L, value );
    }


    public boolean isDynamic() {
        return dynamic;
    }


    public Long getIndex() {
        return index;
    }


    public T getValue() {
        return value;
    }

}

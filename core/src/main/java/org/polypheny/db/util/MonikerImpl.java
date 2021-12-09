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
 */

package org.polypheny.db.util;


import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.algebra.constant.MonikerType;


/**
 * A generic implementation of {@link Moniker}.
 */
public class MonikerImpl implements Moniker {

    private final ImmutableList<String> names;
    private final MonikerType type;


    /**
     * Creates a moniker with an array of names.
     */
    public MonikerImpl( List<String> names, MonikerType type ) {
        this.names = ImmutableList.copyOf( names );
        this.type = Objects.requireNonNull( type );
    }


    /**
     * Creates a moniker with a single name.
     */
    public MonikerImpl( String name, MonikerType type ) {
        this( ImmutableList.of( name ), type );
    }


    @Override
    public boolean equals( Object obj ) {
        return this == obj
                || obj instanceof MonikerImpl
                && type == ((MonikerImpl) obj).type
                && names.equals( ((MonikerImpl) obj).names );
    }


    @Override
    public int hashCode() {
        return Objects.hash( type, names );
    }


    @Override
    public MonikerType getType() {
        return type;
    }


    @Override
    public List<String> getFullyQualifiedNames() {
        return names;
    }


    public String toString() {
        return Util.sepList( names, "." );
    }


    @Override
    public String id() {
        return type + "(" + this + ")";
    }

}


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

package org.polypheny.db.runtime;


import java.util.Iterator;
import java.util.List;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Utility methods called by generated code.
 */
public class Utilities {

    // Even though this is a utility class (all methods are static), we cannot make the constructor private. Because Janino doesn't do static import, generated code is placed in sub-classes.
    protected Utilities() {
    }


    public static int hash( Object v ) {
        return v == null ? 0 : v.hashCode();
    }


    public static int hash( int h, boolean v ) {
        return h * 31 + Boolean.hashCode( v );
    }


    public static int hash( int h, byte v ) {
        return h * 31 + v;
    }


    public static int hash( int h, char v ) {
        return h * 31 + v;
    }


    public static int hash( int h, short v ) {
        return h * 31 + v;
    }


    public static int hash( int h, int v ) {
        return h * 31 + v;
    }


    public static int hash( int h, long v ) {
        return h * 31 + Long.hashCode( v );
    }


    public static int hash( int h, float v ) {
        return hash( h, Float.hashCode( v ) );
    }


    public static int hash( int h, double v ) {
        return hash( h, Double.hashCode( v ) );
    }


    public static int hash( int h, Object v ) {
        return h * 31 + (v == null ? 1 : v.hashCode());
    }


    public static int compare( boolean v0, boolean v1 ) {
        return Boolean.compare( v0, v1 );
    }


    public static int compare( byte v0, byte v1 ) {
        return Byte.compare( v0, v1 );
    }


    public static int compare( char v0, char v1 ) {
        return Character.compare( v0, v1 );
    }


    public static int compare( short v0, short v1 ) {
        return Short.compare( v0, v1 );
    }


    public static int compare( int v0, int v1 ) {
        return Integer.compare( v0, v1 );
    }


    public static int compare( long v0, long v1 ) {
        return Long.compare( v0, v1 );
    }


    public static int compare( float v0, float v1 ) {
        return Float.compare( v0, v1 );
    }


    public static int compare( double v0, double v1 ) {
        return Double.compare( v0, v1 );
    }


    public static int compare( List v0, List v1 ) {
        final Iterator iterator0 = v0.iterator();
        final Iterator iterator1 = v1.iterator();
        for ( ; ; ) {
            if ( !iterator0.hasNext() ) {
                return !iterator1.hasNext()
                        ? 0
                        : -1;
            }
            if ( !iterator1.hasNext() ) {
                return 1;
            }
            final Object o0 = iterator0.next();
            final Object o1 = iterator1.next();
            int c = compare_( o0, o1 );
            if ( c != 0 ) {
                return c;
            }
        }
    }


    private static int compare_( Object o0, Object o1 ) {
        if ( o0 instanceof Comparable ) {
            return compare( (Comparable) o0, (Comparable) o1 );
        }
        return compare( (List) o0, (List) o1 );
    }


    public static int compare( Comparable v0, Comparable v1 ) {
        //noinspection unchecked
        return v0.compareTo( v1 );
    }


    public static int compare( PolyValue v0, PolyValue v1 ) {
        return v0.compareTo( v1 );
    }


    public static int compareNullsFirst( Comparable v0, Comparable v1 ) {
        //noinspection unchecked
        return v0 == v1
                ? 0
                : v0 == null
                        ? -1
                        : v1 == null
                                ? 1
                                : v0.compareTo( v1 );
    }


    public static int compareNullsLast( Comparable v0, Comparable v1 ) {
        //noinspection unchecked
        return v0 == v1
                ? 0
                : v0 == null
                        ? 1
                        : v1 == null
                                ? -1
                                : v0.compareTo( v1 );
    }


    public static <L extends Comparable<L>, R extends Comparable<R>> int compareNullsLast( ComparableList<L> v0, ComparableList<R> v1 ) {
        //noinspection unchecked
        return v0 == v1
                ? 0
                : v0 == null
                        ? 1
                        : v1 == null
                                ? -1
                                : v0.compareTo( (ComparableList<L>) v1 );
    }

}


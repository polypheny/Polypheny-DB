/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.rel.core;


import com.google.common.collect.ImmutableSet;
import java.util.Set;


/**
 * Describes the necessary parameters for an implementation in order to identify and set dynamic variables
 */
public class CorrelationId implements Cloneable, Comparable<CorrelationId> {

    /**
     * Prefix to the name of correlating variables.
     */
    public static final String CORREL_PREFIX = "$cor";

    private final int id;
    private final String name;


    /**
     * Creates a correlation identifier.
     */
    private CorrelationId( int id, String name ) {
        this.id = id;
        this.name = name;
    }


    /**
     * Creates a correlation identifier. This is a type-safe wrapper over int.
     *
     * @param id Identifier
     */
    public CorrelationId( int id ) {
        this( id, CORREL_PREFIX + id );
    }


    /**
     * Creates a correlation identifier from a name.
     *
     * @param name variable name
     */
    public CorrelationId( String name ) {
        this( Integer.parseInt( name.substring( CORREL_PREFIX.length() ) ), name );
        assert name.startsWith( CORREL_PREFIX ) : "Correlation name should start with " + CORREL_PREFIX + " actual name is " + name;
    }


    /**
     * Returns the identifier.
     *
     * @return identifier
     */
    public int getId() {
        return id;
    }


    /**
     * Returns the preferred name of the variable.
     *
     * @return name
     */
    public String getName() {
        return name;
    }


    public String toString() {
        return name;
    }


    public int compareTo( CorrelationId other ) {
        return id - other.id;
    }


    @Override
    public int hashCode() {
        return id;
    }


    @Override
    public boolean equals( Object obj ) {
        return this == obj || obj instanceof CorrelationId && this.id == ((CorrelationId) obj).id;
    }


    /**
     * Converts a set of correlation ids to a set of names.
     */
    public static ImmutableSet<CorrelationId> setOf( Set<String> set ) {
        if ( set.isEmpty() ) {
            return ImmutableSet.of();
        }
        final ImmutableSet.Builder<CorrelationId> builder = ImmutableSet.builder();
        for ( String s : set ) {
            builder.add( new CorrelationId( s ) );
        }
        return builder.build();
    }


    /**
     * Converts a set of names to a set of correlation ids.
     */
    public static Set<String> names( Set<CorrelationId> set ) {
        if ( set.isEmpty() ) {
            return ImmutableSet.of();
        }
        final ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for ( CorrelationId s : set ) {
            builder.add( s.name );
        }
        return builder.build();
    }
}


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

package ch.unibas.dmi.dbis.polyphenydb.schema;


import ch.unibas.dmi.dbis.polyphenydb.materialize.Lattice;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import com.google.common.collect.ImmutableList;


/**
 * Extension to the {@link Schema} interface.
 *
 * Given a user-defined schema that implements the {@link Schema} interface, Polypheny-DB creates a wrapper that implements the {@code SchemaPlus} interface.
 * This provides extra functionality, such as access to tables that have been added explicitly.
 *
 * A user-defined schema does not need to implement this interface, but by the time a schema is passed to a method in a user-defined schema or user-defined table, it will have been wrapped in this interface.
 *
 * SchemaPlus is intended to be used by users but not instantiated by them.
 * Users should only use the SchemaPlus they are given by the system.
 * The purpose of SchemaPlus is to expose to user code, in a read only manner, some of the extra information about schemas that Polypheny-DB builds up when a schema is registered.
 * It appears in several SPI calls as context; for example {@link SchemaFactory#create(SchemaPlus, String, java.util.Map)} contains a parent schema that might be a wrapped instance of a user-defined {@link Schema}, or indeed might not.
 */
public interface SchemaPlus extends Schema {

    /**
     * Returns the parent schema, or null if this schema has no parent.
     */
    SchemaPlus getParentSchema();

    /**
     * Returns the name of this schema.
     *
     * The name must not be null, and must be unique within its parent.
     * The root schema is typically named "".
     */
    String getName();

    // override with stricter return
    SchemaPlus getSubSchema( String name );

    /**
     * Adds a schema as a sub-schema of this schema, and returns the wrapped object.
     */
    SchemaPlus add( String name, Schema schema );

    /**
     * Adds a table to this schema.
     */
    void add( String name, Table table );

    /**
     * Adds a function to this schema.
     */
    void add( String name, Function function );

    /**
     * Adds a type to this schema.
     */
    void add( String name, RelProtoDataType type );

    /**
     * Adds a lattice to this schema.
     */
    void add( String name, Lattice lattice );

    boolean isMutable();

    /**
     * Returns an underlying object.
     */
    <T> T unwrap( Class<T> clazz );

    void setPath( ImmutableList<ImmutableList<String>> path );

    void setCacheEnabled( boolean cache );

    boolean isCacheEnabled();
}


/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.schema;


import com.google.common.collect.ImmutableList;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.rel.type.RelProtoDataType;


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
 */
public interface SchemaPlus extends Schema {


    PolyphenyDbSchema polyphenyDbSchema();

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
    @Override
    SchemaPlus getSubSchema( String name );

    /**
     * Adds a schema as a sub-schema of this schema, and returns the wrapped object.
     */
    SchemaPlus add( String name, Schema schema, SchemaType schemaType );

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

    @Override
    boolean isMutable();

    /**
     * Returns an underlying object.
     */
    <T> T unwrap( Class<T> clazz );

    void setPath( ImmutableList<ImmutableList<String>> path );

    void setCacheEnabled( boolean cache );

    boolean isCacheEnabled();
}


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

package org.polypheny.db.schema.impl;


import java.util.Collection;
import java.util.Set;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.schema.Function;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.SchemaVersion;
import org.polypheny.db.schema.Table;


/**
 * Implementation of {@link Schema} that delegates to an underlying schema.
 */
public class DelegatingSchema implements Schema {

    protected final Schema schema;


    /**
     * Creates a DelegatingSchema.
     *
     * @param schema Underlying schema
     */
    public DelegatingSchema( Schema schema ) {
        this.schema = schema;
    }


    @Override
    public String toString() {
        return "DelegatingSchema(delegate=" + schema + ")";
    }


    @Override
    public boolean isMutable() {
        return schema.isMutable();
    }


    @Override
    public Schema snapshot( SchemaVersion version ) {
        return schema.snapshot( version );
    }


    @Override
    public Expression getExpression( SchemaPlus parentSchema, String name ) {
        return schema.getExpression( parentSchema, name );
    }


    @Override
    public Table getTable( String name ) {
        return schema.getTable( name );
    }


    @Override
    public Set<String> getTableNames() {
        return schema.getTableNames();
    }


    @Override
    public AlgProtoDataType getType( String name ) {
        return schema.getType( name );
    }


    @Override
    public Set<String> getTypeNames() {
        return schema.getTypeNames();
    }


    @Override
    public Collection<Function> getFunctions( String name ) {
        return schema.getFunctions( name );
    }


    @Override
    public Set<String> getFunctionNames() {
        return schema.getFunctionNames();
    }


    @Override
    public Schema getSubSchema( String name ) {
        return schema.getSubSchema( name );
    }


    @Override
    public Set<String> getSubSchemaNames() {
        return schema.getSubSchemaNames();
    }

}


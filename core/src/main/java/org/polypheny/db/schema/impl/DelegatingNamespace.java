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
import org.polypheny.db.schema.Entity;
import org.polypheny.db.schema.Function;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.SchemaVersion;


/**
 * Implementation of {@link Namespace} that delegates to an underlying schema.
 */
public class DelegatingNamespace implements Namespace {

    protected final Namespace namespace;


    /**
     * Creates a DelegatingSchema.
     *
     * @param namespace Underlying schema
     */
    public DelegatingNamespace( Namespace namespace ) {
        this.namespace = namespace;
    }


    @Override
    public String toString() {
        return "DelegatingSchema(delegate=" + namespace + ")";
    }


    @Override
    public boolean isMutable() {
        return namespace.isMutable();
    }


    @Override
    public Namespace snapshot( SchemaVersion version ) {
        return namespace.snapshot( version );
    }


    @Override
    public Expression getExpression( SchemaPlus parentSchema, String name ) {
        return namespace.getExpression( parentSchema, name );
    }


    @Override
    public Entity getEntity( String name ) {
        return namespace.getEntity( name );
    }


    @Override
    public Set<String> getEntityNames() {
        return namespace.getEntityNames();
    }


    @Override
    public AlgProtoDataType getType( String name ) {
        return namespace.getType( name );
    }


    @Override
    public Set<String> getTypeNames() {
        return namespace.getTypeNames();
    }


    @Override
    public Collection<Function> getFunctions( String name ) {
        return namespace.getFunctions( name );
    }


    @Override
    public Set<String> getFunctionNames() {
        return namespace.getFunctionNames();
    }


    @Override
    public long getId() {
        return namespace.getId();
    }


    @Override
    public Namespace getSubNamespace( String name ) {
        return namespace.getSubNamespace( name );
    }


    @Override
    public Set<String> getSubNamespaceNames() {
        return namespace.getSubNamespaceNames();
    }

}


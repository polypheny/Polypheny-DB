/*
 * Copyright 2019-2022 The Polypheny Project
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


import java.util.Collection;
import java.util.Set;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.util.Wrapper;


/**
 * A namespace for tables and functions.
 * <p>
 * A namespace can also contain sub-namespaces, to any level of nesting. Most providers have a limited number of levels; for
 * example, most JDBC databases have either one level ("namespaces") or two levels ("database" and "catalog").
 * <p>
 * There may be multiple overloaded functions with the same name but different numbers or types of parameters. For this reason,
 * {@link #getFunctions} returns a list of all members with the same name. Polypheny-DB will call
 * to choose the appropriate one.
 * <p>
 * The most common and important type of member is the one with no arguments and a result type that is a collection of records.
 * This is called a <dfn>relation</dfn>. It is equivalent to a table in a relational database.
 * <p>
 * For example, the query
 *
 * <blockquote>select * from sales.emps</blockquote>
 *
 * is valid if "sales" is a registered namespace and "emps" is a member with zero parameters and a result type of
 * <code>Collection(Record(int: "empno", String: "name"))</code>.
 * <p>
 * A namespace may be nested within another namespace; see {@link Namespace#getSubNamespace(String)}.
 */
public interface Namespace extends Wrapper {

    long getId();

    default Long getAdapterId() {
        return null;
    }

    /**
     * Returns a sub-namespace with a given name, or null.
     *
     * @param name Sub-namespace name
     * @return Sub-namespace with a given name, or null
     */
    Namespace getSubNamespace( String name );

    /**
     * Returns the names of this namespace's child namespaces.
     *
     * @return Names of this namespace's child namespaces
     */
    Set<String> getSubNamespaceNames();

    /**
     * Returns a table with a given name, or null if not found.
     *
     * @param name Entity name
     * @return Entity, or null
     */
    Entity getEntity( String name );

    /**
     * Returns the names of the tables in this namespace.
     *
     * @return Names of the tables in this namespace
     */
    Set<String> getEntityNames();

    /**
     * Returns a type with a given name, or null if not found.
     *
     * @param name Table name
     * @return Table, or null
     */
    AlgProtoDataType getType( String name );

    /**
     * Returns the names of the types in this namespace.
     *
     * @return Names of the tables in this namespace
     */
    Set<String> getTypeNames();

    /**
     * Returns a list of functions in this namespace with the given name, or an empty list if there is no such function.
     *
     * @param name Name of function
     * @return List of functions with given name, or empty list
     */
    Collection<Function> getFunctions( String name );

    /**
     * Returns the names of the functions in this namespace.
     *
     * @return Names of the functions in this namespace
     */
    Set<String> getFunctionNames();

    /**
     * Returns the expression by which this namespace can be referenced in generated code.
     *
     * @param snapshot Parent namespace
     * @return Expression by which this namespace can be referenced in generated code
     */
    Expression getExpression( Snapshot snapshot, long id );

    /**
     * Returns whether the user is allowed to create new tables, functions and sub-namespaces in this namespace, in addition to
     * those returned automatically by methods such as {@link Schema#getEntity(String)}.
     * <p>
     * Even if this method returns true, the maps are not modified. Polypheny-DB stores the defined objects in a wrapper object.
     *
     * @return Whether the user is allowed to create new tables, functions and sub-namespaces in this namespace
     */
    boolean isMutable();

    /**
     * Returns the snapshot of this namespace as of the specified time. The contents of the namespace snapshot should not change
     * over time.
     *
     * @param version The current namespace version
     * @return the namespace snapshot.
     */
    Namespace snapshot( SchemaVersion version );

    Convention getConvention();

    interface Graph {

    }


    interface Schema {


    }


    interface Database {

    }

}


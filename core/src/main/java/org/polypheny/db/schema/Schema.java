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
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgProtoDataType;


/**
 * A namespace for tables and functions.
 *
 * A schema can also contain sub-schemas, to any level of nesting. Most providers have a limited number of levels; for
 * example, most JDBC databases have either one level ("schemas") or two levels ("database" and "catalog").
 *
 * There may be multiple overloaded functions with the same name but different numbers or types of parameters. For this reason,
 * {@link #getFunctions} returns a list of all members with the same name. Polypheny-DB will call
 * {@link Schemas#resolve(AlgDataTypeFactory, String, java.util.Collection, java.util.List)}
 * to choose the appropriate one.
 *
 * The most common and important type of member is the one with no arguments and a result type that is a collection of records.
 * This is called a <dfn>relation</dfn>. It is equivalent to a table in a relational database.
 *
 * For example, the query
 *
 * <blockquote>select * from sales.emps</blockquote>
 *
 * is valid if "sales" is a registered schema and "emps" is a member with zero parameters and a result type of
 * <code>Collection(Record(int: "empno", String: "name"))</code>.
 *
 * A schema may be nested within another schema; see {@link Schema#getSubSchema(String)}.
 */
public interface Schema {

    /**
     * Returns a table with a given name, or null if not found.
     *
     * @param name Table name
     * @return Table, or null
     */
    Table getTable( String name );

    /**
     * Returns the names of the tables in this schema.
     *
     * @return Names of the tables in this schema
     */
    Set<String> getTableNames();


    /**
     * Returns a type with a given name, or null if not found.
     *
     * @param name Table name
     * @return Table, or null
     */
    AlgProtoDataType getType( String name );

    /**
     * Returns the names of the types in this schema.
     *
     * @return Names of the tables in this schema
     */
    Set<String> getTypeNames();

    /**
     * Returns a list of functions in this schema with the given name, or an empty list if there is no such function.
     *
     * @param name Name of function
     * @return List of functions with given name, or empty list
     */
    Collection<Function> getFunctions( String name );

    /**
     * Returns the names of the functions in this schema.
     *
     * @return Names of the functions in this schema
     */
    Set<String> getFunctionNames();

    /**
     * Returns a sub-schema with a given name, or null.
     *
     * @param name Sub-schema name
     * @return Sub-schema with a given name, or null
     */
    Schema getSubSchema( String name );

    /**
     * Returns the names of this schema's child schemas.
     *
     * @return Names of this schema's child schemas
     */
    Set<String> getSubSchemaNames();

    /**
     * Returns the expression by which this schema can be referenced in generated code.
     *
     * @param parentSchema Parent schema
     * @param name Name of this schema
     * @return Expression by which this schema can be referenced in generated code
     */
    Expression getExpression( SchemaPlus parentSchema, String name );

    /**
     * Returns whether the user is allowed to create new tables, functions and sub-schemas in this schema, in addition to
     * those returned automatically by methods such as {@link #getTable(String)}.
     *
     * Even if this method returns true, the maps are not modified. Polypheny-DB stores the defined objects in a wrapper object.
     *
     * @return Whether the user is allowed to create new tables, functions and sub-schemas in this schema
     */
    boolean isMutable();

    /**
     * Returns the snapshot of this schema as of the specified time. The contents of the schema snapshot should not change
     * over time.
     *
     * @param version The current schema version
     * @return the schema snapshot.
     */
    Schema snapshot( SchemaVersion version );

    /**
     * Table type.
     */
    enum TableType {
        /**
         * A regular table.
         *
         * Used by DB2, MySQL, PostgreSQL and others.
         */
        TABLE,

        /**
         * A relation whose contents are calculated by evaluating a SQL expression.
         *
         * Used by DB2, PostgreSQL and others.
         */
        VIEW,

        /**
         * Foreign table.
         *
         * Used by PostgreSQL.
         */
        FOREIGN_TABLE,

        /**
         * Index table.
         *
         * Used by Apache Phoenix, PostgreSQL.
         */
        INDEX,

        /**
         * Join table.
         *
         * Used by Apache Phoenix.
         */
        JOIN,

        /**
         * Sequence table.
         *
         * Used by Apache Phoenix, Oracle, PostgreSQL and others.
         * In Phoenix, must have a single BIGINT column called "$seq".
         */
        SEQUENCE,

        /**
         * Stream.
         */
        STREAM,

        /**
         * Type.
         *
         * Used by PostgreSQL.
         */
        TYPE,

        /**
         * A table maintained by the system. Data dictionary tables, such as the "TABLES" and "COLUMNS" table in the "metamodel" schema, examples of system tables.
         *
         * Specified by the JDBC standard and used by DB2, MySQL, Oracle, PostgreSQL and others.
         */
        SYSTEM_TABLE,

        /**
         * System view.
         *
         * Used by PostgreSQL, MySQL.
         */
        SYSTEM_VIEW,

        /**
         * System index.
         *
         * Used by PostgreSQL.
         */
        SYSTEM_INDEX,

        /**
         * System TOAST index.
         *
         * Used by PostgreSQL.
         */
        SYSTEM_TOAST_INDEX,

        /**
         * System TOAST table.
         *
         * Used by PostgreSQL.
         */
        SYSTEM_TOAST_TABLE,

        /**
         * Temporary index.
         *
         * Used by PostgreSQL.
         */
        TEMPORARY_INDEX,

        /**
         * Temporary sequence.
         *
         * Used by PostgreSQL.
         */
        TEMPORARY_SEQUENCE,

        /**
         * Temporary table.
         *
         * Used by PostgreSQL.
         */
        TEMPORARY_TABLE,

        /**
         * Temporary view.
         *
         * Used by PostgreSQL.
         */
        TEMPORARY_VIEW,

        /**
         * A table that is only visible to one connection.
         *
         * Specified by the JDBC standard and used by PostgreSQL, MySQL.
         */
        LOCAL_TEMPORARY,

        /**
         * A synonym.
         *
         * Used by DB2, Oracle.
         */
        SYNONYM,

        /**
         * An alias.
         *
         * Specified by the JDBC standard.
         */
        ALIAS,

        /**
         * A global temporary table.
         *
         * Specified by the JDBC standard.
         */
        GLOBAL_TEMPORARY,

        /**
         * An accel-only table.
         *
         * Used by DB2.
         */
        ACCEL_ONLY_TABLE,

        /**
         * An auxiliary table.
         *
         * Used by DB2.
         */
        AUXILIARY_TABLE,

        /**
         * A global temporary table.
         *
         * Used by DB2.
         */
        GLOBAL_TEMPORARY_TABLE,

        /**
         * A hierarchy table.
         *
         * Used by DB2.
         */
        HIERARCHY_TABLE,

        /**
         * An inoperative view.
         *
         * Used by DB2.
         */
        INOPERATIVE_VIEW,

        /**
         * A nickname.
         *
         * Used by DB2.
         */
        NICKNAME,

        /**
         * A typed table.
         *
         * Used by DB2.
         */
        TYPED_TABLE,

        /**
         * A typed view.
         *
         * Used by DB2.
         */
        TYPED_VIEW,

        /**
         * Table type not known to Polypheny-DB.
         *
         * If you get one of these, please fix the problem by adding an enum value.
         */
        OTHER;

        /**
         * The name used in JDBC. For example "SYSTEM TABLE" rather than "SYSTEM_TABLE".
         */
        public final String jdbcName;


        TableType() {
            this.jdbcName = name().replace( '_', ' ' );
        }
    }

}


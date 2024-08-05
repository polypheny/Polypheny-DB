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

package org.polypheny.db.schema;

/**
 * Table type.
 */
public enum TableType {
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

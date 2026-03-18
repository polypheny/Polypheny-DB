/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.postgres;

/**
 * Utility class holding raw SQL queries used to interrogate the PostgreSQL system catalog.
 */
public final class PostgresqlCatalogQueries {

    /**
    SQL to query postgres system catalog attribute modifier count.
    a.attnum > 0: filters out hidden system columns with attnum < 0
    a.attisdropped: marked but not yet removed columns
    a.atttypmod > 0: attribute type modifier was used i.e. vector(atttymod), otherwise atttypmod = -1
     */
    public static final String SQL_COLUMN_TYPE_MODIFIERS_AND_ATTR_DIMENSIONS = """
          SELECT a.attname, a.atttypmod, a.attndims
          FROM pg_attribute a
          JOIN pg_class     c ON a.attrelid     = c.oid
          JOIN pg_namespace n ON c.relnamespace = n.oid
          WHERE c.relname = ? AND n.nspname = ?
            AND a.attnum > 0 AND NOT a.attisdropped
            AND (a.attndims > 0 OR a.atttypmod > 0)
          """;


    /**
     * Retrieves a list of all currently installed and active extensions in the database.
     */
    public static final String SQL_INSTALLED_EXTENSIONS = """
            SELECT extname
            FROM pg_extension
            """;
}

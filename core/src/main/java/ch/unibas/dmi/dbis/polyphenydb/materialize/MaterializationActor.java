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

package ch.unibas.dmi.dbis.polyphenydb.materialize;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * Actor that manages the state of materializations in the system.
 */
class MaterializationActor {
    // Not an actor yet -- TODO make members private and add request/response queues

    final Map<MaterializationKey, Materialization> keyMap = new HashMap<>();

    final Map<QueryKey, MaterializationKey> keyBySql = new HashMap<>();

    final Map<TileKey, MaterializationKey> keyByTile = new HashMap<>();

    /**
     * Tiles grouped by dimensionality. We use a {@link TileKey} with no measures to represent a dimensionality.
     */
    final Multimap<TileKey, TileKey> tilesByDimensionality = HashMultimap.create();


    /**
     * A query materialized in a table, so that reading from the table gives the same results as executing the query.
     */
    static class Materialization {

        final MaterializationKey key;
        final PolyphenyDbSchema rootSchema;
        PolyphenyDbSchema.TableEntry materializedTable;
        final String sql;
        final RelDataType rowType;
        final List<String> viewSchemaPath;


        /**
         * Creates a materialization.
         *
         * @param key Unique identifier of this materialization
         * @param materializedTable Table that currently materializes the query. That is, executing "select * from table" will give the same results as executing the query. May be null when the materialization is created; materialization service will change the value as
         * @param sql Query that is materialized
         * @param rowType Row type
         */
        Materialization( MaterializationKey key, PolyphenyDbSchema rootSchema, PolyphenyDbSchema.TableEntry materializedTable, String sql, RelDataType rowType, List<String> viewSchemaPath ) {
            this.key = key;
            this.rootSchema = Objects.requireNonNull( rootSchema );
            Preconditions.checkArgument( rootSchema.isRoot(), "must be root schema" );
            this.materializedTable = materializedTable; // may be null
            this.sql = sql;
            this.rowType = rowType;
            this.viewSchemaPath = viewSchemaPath;
        }
    }


    /**
     * A materialization can be re-used if it is the same SQL, on the same schema, with the same path for resolving functions.
     */
    static class QueryKey {

        final String sql;
        final PolyphenyDbSchema schema;
        final List<String> path;


        QueryKey( String sql, PolyphenyDbSchema schema, List<String> path ) {
            this.sql = sql;
            this.schema = schema;
            this.path = path;
        }


        @Override
        public boolean equals( Object obj ) {
            return obj == this
                    || obj instanceof QueryKey
                    && sql.equals( ((QueryKey) obj).sql )
                    && schema.equals( ((QueryKey) obj).schema )
                    && path.equals( ((QueryKey) obj).path );
        }


        @Override
        public int hashCode() {
            return Objects.hash( sql, schema, path );
        }
    }
}


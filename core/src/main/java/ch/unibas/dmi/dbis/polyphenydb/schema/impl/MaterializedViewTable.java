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

package ch.unibas.dmi.dbis.polyphenydb.schema.impl;


import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbConnection;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.ParseResult;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.materialize.MaterializationKey;
import ch.unibas.dmi.dbis.polyphenydb.materialize.MaterializationService;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schemas;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.schema.TranslatableTable;
import java.lang.reflect.Type;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;


/**
 * Table that is a materialized view.
 *
 * It can exist in two states: materialized and not materialized. Over time, a given materialized view may switch states. How it is expanded depends upon its current state. State is managed by
 * {@link MaterializationService}.
 */
public class MaterializedViewTable extends ViewTable {

    private final MaterializationKey key;

    /**
     * Internal connection, used to execute queries to materialize views.
     * To be used only by Polypheny-DB internals. And sparingly.
     */
    public static final PolyphenyDbConnection MATERIALIZATION_CONNECTION;


    static {
        try {
            MATERIALIZATION_CONNECTION = DriverManager.getConnection( "jdbc:polyphenydb:" ).unwrap( PolyphenyDbConnection.class );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    public MaterializedViewTable( Type elementType, RelProtoDataType relDataType, String viewSql, List<String> viewSchemaPath, List<String> viewPath, MaterializationKey key ) {
        super( elementType, relDataType, viewSql, viewSchemaPath, viewPath );
        this.key = key;
    }


    /**
     * Table macro that returns a materialized view.
     */
    public static MaterializedViewTableMacro create( final PolyphenyDbSchema schema, final String viewSql, final List<String> viewSchemaPath, List<String> viewPath, final String suggestedTableName, boolean existing ) {
        return new MaterializedViewTableMacro( schema, viewSql, viewSchemaPath, viewPath, suggestedTableName, existing );
    }


    @Override
    public RelNode toRel( RelOptTable.ToRelContext context, RelOptTable relOptTable ) {
        final PolyphenyDbSchema.TableEntry tableEntry = MaterializationService.instance().checkValid( key );
        if ( tableEntry != null ) {
            Table materializeTable = tableEntry.getTable();
            if ( materializeTable instanceof TranslatableTable ) {
                TranslatableTable table = (TranslatableTable) materializeTable;
                return table.toRel( context, relOptTable );
            }
        }
        return super.toRel( context, relOptTable );
    }


    /**
     * Table function that returns the table that materializes a view.
     */
    public static class MaterializedViewTableMacro extends ViewTableMacro {

        private final MaterializationKey key;


        private MaterializedViewTableMacro( PolyphenyDbSchema schema, String viewSql, List<String> viewSchemaPath, List<String> viewPath, String suggestedTableName, boolean existing ) {
            super( schema, viewSql, viewSchemaPath != null ? viewSchemaPath : schema.path( null ), viewPath, Boolean.TRUE );
            this.key = Objects.requireNonNull( MaterializationService.instance().defineMaterialization( schema, null, viewSql, schemaPath, suggestedTableName, true, existing ) );
        }


        @Override
        public TranslatableTable apply( List<Object> arguments ) {
            assert arguments.isEmpty();
            ParseResult parsed = Schemas.parse( MATERIALIZATION_CONNECTION, schema, schemaPath, viewSql );
            final List<String> schemaPath1 = schemaPath != null ? schemaPath : schema.path( null );
            final JavaTypeFactory typeFactory = MATERIALIZATION_CONNECTION.getTypeFactory();
            return new MaterializedViewTable( typeFactory.getJavaClass( parsed.rowType ), RelDataTypeImpl.proto( parsed.rowType ), viewSql, schemaPath1, viewPath, key );
        }
    }
}


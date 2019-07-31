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


import ch.unibas.dmi.dbis.polyphenydb.adapter.java.AbstractQueryableTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelRoot;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelShuttleImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema.TableType;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.TranslatableTable;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import java.util.List;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;


/**
 * Table whose contents are defined using an SQL statement.
 *
 * It is not evaluated; it is expanded during query planning.
 */
public class ViewTable extends AbstractQueryableTable implements TranslatableTable {

    private final String viewSql;
    private final List<String> schemaPath;
    private final RelProtoDataType protoRowType;
    private final List<String> viewPath;


    public ViewTable( Type elementType, RelProtoDataType rowType, String viewSql, List<String> schemaPath, List<String> viewPath ) {
        super( elementType );
        this.viewSql = viewSql;
        this.schemaPath = ImmutableList.copyOf( schemaPath );
        this.protoRowType = rowType;
        this.viewPath = viewPath == null ? null : ImmutableList.copyOf( viewPath );
    }


    @Deprecated // to be removed before 2.0
    public static ViewTableMacro viewMacro( SchemaPlus schema, final String viewSql, final List<String> schemaPath ) {
        return viewMacro( schema, viewSql, schemaPath, null, Boolean.TRUE );
    }


    @Deprecated // to be removed before 2.0
    public static ViewTableMacro viewMacro( SchemaPlus schema, String viewSql, List<String> schemaPath, Boolean modifiable ) {
        return viewMacro( schema, viewSql, schemaPath, null, modifiable );
    }


    /**
     * Table macro that returns a view.
     *
     * @param schema Schema the view will belong to
     * @param viewSql SQL query
     * @param schemaPath Path of schema
     * @param modifiable Whether view is modifiable, or null to deduce it
     */
    public static ViewTableMacro viewMacro( SchemaPlus schema, String viewSql, List<String> schemaPath, List<String> viewPath, Boolean modifiable ) {
        return new ViewTableMacro( PolyphenyDbSchema.from( schema ), viewSql, schemaPath, viewPath, modifiable );
    }


    /**
     * Returns the view's SQL definition.
     */
    public String getViewSql() {
        return viewSql;
    }


    /**
     * Returns the the schema path of the view.
     */
    public List<String> getSchemaPath() {
        return schemaPath;
    }


    /**
     * Returns the the path of the view.
     */
    public List<String> getViewPath() {
        return viewPath;
    }


    @Override
    public TableType getJdbcTableType() {
        return Schema.TableType.VIEW;
    }


    public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
        return protoRowType.apply( typeFactory );
    }


    public <T> Queryable<T> asQueryable( QueryProvider queryProvider, SchemaPlus schema, String tableName ) {
        return queryProvider.createQuery( getExpression( schema, tableName, Queryable.class ), elementType );
    }


    public RelNode toRel( RelOptTable.ToRelContext context, RelOptTable relOptTable ) {
        return expandView( context, relOptTable.getRowType(), viewSql ).rel;
    }


    private RelRoot expandView( RelOptTable.ToRelContext context, RelDataType rowType, String queryString ) {
        try {
            final RelRoot root = context.expandView( rowType, queryString, schemaPath, viewPath );
            final RelNode rel = RelOptUtil.createCastRel( root.rel, rowType, true );
            // Expand any views
            final RelNode rel2 = rel.accept(
                    new RelShuttleImpl() {
                        @Override
                        public RelNode visit( TableScan scan ) {
                            final RelOptTable table = scan.getTable();
                            final TranslatableTable translatableTable = table.unwrap( TranslatableTable.class );
                            if ( translatableTable != null ) {
                                return translatableTable.toRel( context, table );
                            }
                            return super.visit( scan );
                        }
                    } );
            return root.withRel( rel2 );
        } catch ( Exception e ) {
            throw new RuntimeException( "Error while parsing view definition: " + queryString, e );
        }
    }
}


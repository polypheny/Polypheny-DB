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

package org.polypheny.db.schema.impl;


import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import java.util.List;
import org.apache.calcite.linq4j.Queryable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.AbstractQueryableTable;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.RelShuttleImpl;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelProtoDataType;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.Schema.TableType;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.TranslatableTable;


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
     * Returns the schema path of the view.
     */
    public List<String> getSchemaPath() {
        return schemaPath;
    }


    /**
     * Returns the path of the view.
     */
    public List<String> getViewPath() {
        return viewPath;
    }


    @Override
    public TableType getJdbcTableType() {
        return Schema.TableType.VIEW;
    }


    @Override
    public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
        return protoRowType.apply( typeFactory );
    }


    @Override
    public <T> Queryable<T> asQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
        return dataContext.getQueryProvider().createQuery( getExpression( schema, tableName, Queryable.class ), elementType );
    }


    @Override
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


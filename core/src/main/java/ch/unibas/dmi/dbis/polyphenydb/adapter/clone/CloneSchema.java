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

package ch.unibas.dmi.dbis.polyphenydb.adapter.clone;


import static ch.unibas.dmi.dbis.polyphenydb.schema.impl.MaterializedViewTable.MATERIALIZATION_CONNECTION;

import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.JdbcSchema;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbEmbeddedConnection;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollations;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import ch.unibas.dmi.dbis.polyphenydb.schema.QueryableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaFactory;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schemas;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractSchema;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;


/**
 * Schema that contains in-memory copies of tables from a JDBC schema.
 */
public class CloneSchema extends AbstractSchema {
    // TODO: implement 'driver' property
    // TODO: implement 'source' property
    // TODO: test Factory

    private final SchemaPlus sourceSchema;


    /**
     * Creates a CloneSchema.
     *
     * @param sourceSchema JDBC data source
     */
    public CloneSchema( SchemaPlus sourceSchema ) {
        super();
        this.sourceSchema = sourceSchema;
    }


    @Override
    protected Map<String, Table> getTableMap() {
        final Map<String, Table> map = new LinkedHashMap<>();
        for ( String name : sourceSchema.getTableNames() ) {
            final Table table = sourceSchema.getTable( name );
            if ( table instanceof QueryableTable ) {
                final QueryableTable sourceTable = (QueryableTable) table;
                map.put( name, createCloneTable( MATERIALIZATION_CONNECTION, sourceTable, name ) );
            }
        }
        return map;
    }


    private Table createCloneTable( QueryProvider queryProvider, QueryableTable sourceTable, String name ) {
        final Queryable<Object> queryable = sourceTable.asQueryable( queryProvider, sourceSchema, name );
        final JavaTypeFactory typeFactory = ((PolyphenyDbEmbeddedConnection) queryProvider).getTypeFactory();
        return createCloneTable( typeFactory, Schemas.proto( sourceTable ), ImmutableList.of(), null, queryable );
    }


    @Deprecated // to be removed before 2.0
    public static <T> Table createCloneTable( final JavaTypeFactory typeFactory, final RelProtoDataType protoRowType, final List<ColumnMetaData.Rep> repList, final Enumerable<T> source ) {
        return createCloneTable( typeFactory, protoRowType, ImmutableList.of(), repList, source );
    }


    public static <T> Table createCloneTable( final JavaTypeFactory typeFactory, final RelProtoDataType protoRowType, final List<RelCollation> collations, final List<ColumnMetaData.Rep> repList, final Enumerable<T> source ) {
        final Type elementType;
        if ( source instanceof QueryableTable ) {
            elementType = ((QueryableTable) source).getElementType();
        } else if ( protoRowType.apply( typeFactory ).getFieldCount() == 1 ) {
            if ( repList != null ) {
                elementType = repList.get( 0 ).clazz;
            } else {
                elementType = Object.class;
            }
        } else {
            elementType = Object[].class;
        }
        return new ArrayTable(
                elementType,
                protoRowType,
                Suppliers.memoize( () -> {
                    final ColumnLoader loader = new ColumnLoader<>( typeFactory, source, protoRowType, repList );
                    final List<RelCollation> collation2 =
                            collations.isEmpty() && loader.sortField >= 0
                                    ? RelCollations.createSingleton( loader.sortField )
                                    : collations;
                    return new ArrayTable.Content( loader.representationValues, loader.size(), collation2 );
                } ) );
    }


    /**
     * Schema factory that creates a {@link ch.unibas.dmi.dbis.polyphenydb.adapter.clone.CloneSchema}.
     * This allows you to create a clone schema inside a model.json file.
     *
     * <blockquote><pre>
     * {
     *   version: '1.0',
     *   defaultSchema: 'FOODMART_CLONE',
     *   schemas: [
     *     {
     *       name: 'FOODMART_CLONE',
     *       type: 'custom',
     *       factory: 'ch.unibas.dmi.dbis.polyphenydb.adapter.clone.CloneSchema$Factory',
     *       operand: {
     *         jdbcDriver: 'com.mysql.jdbc.Driver',
     *         jdbcUrl: 'jdbc:mysql://localhost/foodmart',
     *         jdbcUser: 'foodmart',
     *         jdbcPassword: 'foodmart'
     *       }
     *     }
     *   ]
     * }</pre></blockquote>
     */
    public static class Factory implements SchemaFactory {

        public Schema create( SchemaPlus parentSchema, String name, Map<String, Object> operand ) {
            SchemaPlus schema = parentSchema.add( name, JdbcSchema.create( parentSchema, name + "$source", operand, null ) );
            return new CloneSchema( schema );
        }
    }
}


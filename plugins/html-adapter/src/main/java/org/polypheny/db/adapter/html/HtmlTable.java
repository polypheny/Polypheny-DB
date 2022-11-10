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

package org.polypheny.db.adapter.html;


import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.enumerable.EnumerableScan;
import org.polypheny.db.adapter.java.AbstractQueryableTable;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgOptTable.ToAlgContext;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Statistic;
import org.polypheny.db.schema.Statistics;
import org.polypheny.db.schema.TranslatableTable;
import org.polypheny.db.schema.impl.AbstractTableQueryable;
import org.polypheny.db.util.Source;


/**
 * Table implementation wrapping a URL / HTML table.
 */
class HtmlTable extends AbstractQueryableTable implements TranslatableTable {

    private final AlgProtoDataType protoRowType;
    private HtmlReader reader;
    private HtmlRowConverter converter;


    /**
     * Creates a HtmlTable.
     */
    private HtmlTable( Source source, String selector, Integer index, AlgProtoDataType protoRowType, List<Map<String, Object>> fieldConfigs ) throws Exception {
        super( Object[].class );

        this.protoRowType = protoRowType;
        this.reader = new HtmlReader( source, selector, index );
        this.converter = new HtmlRowConverter( this.reader, fieldConfigs );
    }


    /**
     * Creates a HtmlTable.
     */
    static HtmlTable create( Source source, Map<String, Object> tableDef ) throws Exception {
        @SuppressWarnings("unchecked") List<Map<String, Object>> fieldConfigs = (List<Map<String, Object>>) tableDef.get( "fields" );
        String selector = (String) tableDef.get( "selector" );
        Integer index = (Integer) tableDef.get( "index" );
        return new HtmlTable( source, selector, index, null, fieldConfigs );
    }


    public String toString() {
        return "HtmlTable";
    }


    @Override
    public Statistic getStatistic() {
        return Statistics.UNKNOWN;
    }


    @Override
    public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
        if ( protoRowType != null ) {
            return protoRowType.apply( typeFactory );
        }
        return this.converter.getRowType( (JavaTypeFactory) typeFactory );
    }


    @Override
    public <T> Queryable<T> asQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
        return new AbstractTableQueryable<T>( dataContext, schema, this, tableName ) {
            @Override
            public Enumerator<T> enumerator() {
                try {
                    HtmlEnumerator enumerator = new HtmlEnumerator( reader.iterator(), converter );
                    //noinspection unchecked
                    return (Enumerator<T>) enumerator;
                } catch ( Exception e ) {
                    throw new RuntimeException( e );
                }
            }
        };
    }


    /**
     * Returns an enumerable over a given projection of the fields.
     */
    public Enumerable<Object> project( final int[] fields ) {
        return new AbstractEnumerable<Object>() {
            @Override
            public Enumerator<Object> enumerator() {
                try {
                    return new HtmlEnumerator( reader.iterator(), converter, fields );
                } catch ( Exception e ) {
                    throw new RuntimeException( e );
                }
            }
        };
    }


    @Override
    public AlgNode toAlg( ToAlgContext context, AlgOptTable algOptTable ) {
        return new EnumerableScan( context.getCluster(), context.getCluster().traitSetOf( EnumerableConvention.INSTANCE ), algOptTable, (Class) getElementType() );
    }

}


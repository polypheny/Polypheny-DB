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

package ch.unibas.dmi.dbis.polyphenydb.adapter.file;


import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableConvention;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableTableScan;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.AbstractQueryableTable;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Statistic;
import ch.unibas.dmi.dbis.polyphenydb.schema.Statistics;
import ch.unibas.dmi.dbis.polyphenydb.schema.TranslatableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractTableQueryable;
import ch.unibas.dmi.dbis.polyphenydb.util.Source;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;


/**
 * Table implementation wrapping a URL / HTML table.
 */
class FileTable extends AbstractQueryableTable implements TranslatableTable {

    private final RelProtoDataType protoRowType;
    private FileReader reader;
    private FileRowConverter converter;


    /**
     * Creates a FileTable.
     */
    private FileTable( Source source, String selector, Integer index, RelProtoDataType protoRowType, List<Map<String, Object>> fieldConfigs ) throws Exception {
        super( Object[].class );

        this.protoRowType = protoRowType;
        this.reader = new FileReader( source, selector, index );
        this.converter = new FileRowConverter( this.reader, fieldConfigs );
    }


    /**
     * Creates a FileTable.
     */
    static FileTable create( Source source, Map<String, Object> tableDef ) throws Exception {
        @SuppressWarnings("unchecked") List<Map<String, Object>> fieldConfigs = (List<Map<String, Object>>) tableDef.get( "fields" );
        String selector = (String) tableDef.get( "selector" );
        Integer index = (Integer) tableDef.get( "index" );
        return new FileTable( source, selector, index, null, fieldConfigs );
    }


    public String toString() {
        return "FileTable";
    }


    @Override
    public Statistic getStatistic() {
        return Statistics.UNKNOWN;
    }


    @Override
    public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
        if ( protoRowType != null ) {
            return protoRowType.apply( typeFactory );
        }
        return this.converter.getRowType( (JavaTypeFactory) typeFactory );
    }


    @Override
    public <T> Queryable<T> asQueryable( QueryProvider queryProvider, SchemaPlus schema, String tableName ) {
        return new AbstractTableQueryable<T>( queryProvider, schema, this, tableName ) {
            @Override
            public Enumerator<T> enumerator() {
                try {
                    FileEnumerator enumerator = new FileEnumerator( reader.iterator(), converter );
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
                    return new FileEnumerator( reader.iterator(), converter, fields );
                } catch ( Exception e ) {
                    throw new RuntimeException( e );
                }
            }
        };
    }


    @Override
    public RelNode toRel( RelOptTable.ToRelContext context, RelOptTable relOptTable ) {
        return new EnumerableTableScan( context.getCluster(), context.getCluster().traitSetOf( EnumerableConvention.INSTANCE ), relOptTable, (Class) getElementType() );
    }
}


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
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.schemas;


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollations;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelFieldCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.schema.ScannableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.Statistic;
import ch.unibas.dmi.dbis.polyphenydb.schema.Statistics;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractTable;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;


/**
 * A typical HR schema with employees (emps) and departments (depts) tables that are naturally ordered based on their primary keys representing clustered tables.
 */
public final class HrClusteredSchema extends AbstractSchema {

    private final ImmutableMap<String, Table> tables;


    public HrClusteredSchema() {
        super();
        tables = ImmutableMap.<String, Table>builder()
                .put( "emps",
                        new PkClusteredTable(
                                factory ->
                                        new RelDataTypeFactory.Builder( factory )
                                                .add( "empid", factory.createJavaType( int.class ) )
                                                .add( "deptno", factory.createJavaType( int.class ) )
                                                .add( "name", factory.createJavaType( String.class ) )
                                                .add( "salary", factory.createJavaType( int.class ) )
                                                .add( "commission", factory.createJavaType( Integer.class ) )
                                                .build(),
                                ImmutableBitSet.of( 0 ),
                                Arrays.asList(
                                        new Object[]{ 100, 10, "Bill", 10000, 1000 },
                                        new Object[]{ 110, 10, "Theodore", 11500, 250 },
                                        new Object[]{ 150, 10, "Sebastian", 7000, null },
                                        new Object[]{ 200, 20, "Eric", 8000, 500 } )
                        ) )
                .put( "depts",
                        new PkClusteredTable(
                                factory ->
                                        new RelDataTypeFactory.Builder( factory )
                                                .add( "deptno", factory.createJavaType( int.class ) )
                                                .add( "name", factory.createJavaType( String.class ) )
                                                .build(),
                                ImmutableBitSet.of( 0 ),
                                Arrays.asList(
                                        new Object[]{ 10, "Sales" },
                                        new Object[]{ 30, "Marketing" },
                                        new Object[]{ 40, "HR" } )
                        ) ).build();
    }


    @Override
    protected Map<String, Table> getTableMap() {
        return tables;
    }


    /**
     * A table sorted (ascending direction and nulls last) on the primary key.
     */
    private static class PkClusteredTable extends AbstractTable implements ScannableTable {

        private final ImmutableBitSet pkColumns;
        private final List<Object[]> data;
        private final Function<RelDataTypeFactory, RelDataType> typeBuilder;


        PkClusteredTable( Function<RelDataTypeFactory, RelDataType> dataTypeBuilder, ImmutableBitSet pkColumns, List<Object[]> data ) {
            this.data = data;
            this.typeBuilder = dataTypeBuilder;
            this.pkColumns = pkColumns;
        }


        @Override
        public Statistic getStatistic() {
            List<RelFieldCollation> collationFields = new ArrayList<>();
            for ( Integer key : pkColumns ) {
                collationFields.add( new RelFieldCollation( key, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST ) );
            }
            return Statistics.of( data.size(), ImmutableList.of( pkColumns ), ImmutableList.of( RelCollations.of( collationFields ) ) );
        }


        @Override
        public RelDataType getRowType( final RelDataTypeFactory typeFactory ) {
            return typeBuilder.apply( typeFactory );
        }


        @Override
        public Enumerable<Object[]> scan( final DataContext root ) {
            return Linq4j.asEnumerable( data );
        }

    }
}

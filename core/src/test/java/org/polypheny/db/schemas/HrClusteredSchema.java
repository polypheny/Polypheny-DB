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

package org.polypheny.db.schemas;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.schema.ScannableTable;
import org.polypheny.db.schema.Statistic;
import org.polypheny.db.schema.Statistics;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.impl.AbstractSchema;
import org.polypheny.db.schema.impl.AbstractTable;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * A typical HR schema with employees (emps) and departments (depts) tables that are naturally ordered based on their primary keys representing clustered tables.
 */
public final class HrClusteredSchema extends AbstractSchema {

    private final ImmutableMap<String, Table> tables;


    public HrClusteredSchema() {
        super();
        tables = ImmutableMap.<String, Table>builder()
                .put(
                        "emps",
                        new PkClusteredTable(
                                factory ->
                                        new AlgDataTypeFactory.Builder( factory )
                                                .add( "empid", null, factory.createJavaType( int.class ) )
                                                .add( "deptno", null, factory.createJavaType( int.class ) )
                                                .add( "name", null, factory.createJavaType( String.class ) )
                                                .add( "salary", null, factory.createJavaType( int.class ) )
                                                .add( "commission", null, factory.createJavaType( Integer.class ) )
                                                .build(),
                                ImmutableBitSet.of( 0 ),
                                Arrays.asList(
                                        new Object[]{ 100, 10, "Bill", 10000, 1000 },
                                        new Object[]{ 110, 10, "Theodore", 11500, 250 },
                                        new Object[]{ 150, 10, "Sebastian", 7000, null },
                                        new Object[]{ 200, 20, "Eric", 8000, 500 } )
                        ) )
                .put(
                        "depts",
                        new PkClusteredTable(
                                factory ->
                                        new AlgDataTypeFactory.Builder( factory )
                                                .add( "deptno", null, factory.createJavaType( int.class ) )
                                                .add( "name", null, factory.createJavaType( String.class ) )
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
        private final Function<AlgDataTypeFactory, AlgDataType> typeBuilder;


        PkClusteredTable( Function<AlgDataTypeFactory, AlgDataType> dataTypeBuilder, ImmutableBitSet pkColumns, List<Object[]> data ) {
            this.data = data;
            this.typeBuilder = dataTypeBuilder;
            this.pkColumns = pkColumns;
        }


        @Override
        public Statistic getStatistic() {
            List<AlgFieldCollation> collationFields = new ArrayList<>();
            for ( Integer key : pkColumns ) {
                collationFields.add( new AlgFieldCollation( key, AlgFieldCollation.Direction.ASCENDING, AlgFieldCollation.NullDirection.LAST ) );
            }
            return Statistics.of( Double.valueOf( data.size() ), ImmutableList.of( pkColumns ), ImmutableList.of( AlgCollations.of( collationFields ) ) );
        }


        @Override
        public AlgDataType getRowType( final AlgDataTypeFactory typeFactory ) {
            return typeBuilder.apply( typeFactory );
        }


        @Override
        public Enumerable<Object[]> scan( final DataContext root ) {
            return Linq4j.asEnumerable( data );
        }

    }

}

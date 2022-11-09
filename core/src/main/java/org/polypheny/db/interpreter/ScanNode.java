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

package org.polypheny.db.interpreter;


import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.runtime.Enumerables;
import org.polypheny.db.schema.FilterableTable;
import org.polypheny.db.schema.ProjectableFilterableTable;
import org.polypheny.db.schema.QueryableTable;
import org.polypheny.db.schema.ScannableTable;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.mapping.Mapping;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Interpreter node that implements a {@link Scan}.
 */
public class ScanNode implements Node {

    private ScanNode( Compiler compiler, Scan alg, Enumerable<Row> enumerable ) {
        compiler.enumerable( alg, enumerable );
    }


    @Override
    public void run() {
        // nothing to do
    }


    /**
     * Creates a ScanNode.
     *
     * Tries various table SPIs, and negotiates with the table which filters and projects it can implement. Adds to the Enumerable implementations of any filters and projects that cannot be implemented by the table.
     */
    static ScanNode create( Compiler compiler, Scan alg, ImmutableList<RexNode> filters, ImmutableIntList projects ) {
        final AlgOptTable algOptTable = alg.getTable();
        final ProjectableFilterableTable pfTable = algOptTable.unwrap( ProjectableFilterableTable.class );
        if ( pfTable != null ) {
            return createProjectableFilterable( compiler, alg, filters, projects, pfTable );
        }
        final FilterableTable filterableTable = algOptTable.unwrap( FilterableTable.class );
        if ( filterableTable != null ) {
            return createFilterable( compiler, alg, filters, projects, filterableTable );
        }
        final ScannableTable scannableTable = algOptTable.unwrap( ScannableTable.class );
        if ( scannableTable != null ) {
            return createScannable( compiler, alg, filters, projects, scannableTable );
        }
        //noinspection unchecked
        final Enumerable<Row> enumerable = algOptTable.unwrap( Enumerable.class );
        if ( enumerable != null ) {
            return createEnumerable( compiler, alg, enumerable, null, filters, projects );
        }
        final QueryableTable queryableTable = algOptTable.unwrap( QueryableTable.class );
        if ( queryableTable != null ) {
            return createQueryable( compiler, alg, filters, projects, queryableTable );
        }
        throw new AssertionError( "cannot convert table " + algOptTable + " to enumerable" );
    }


    private static ScanNode createScannable( Compiler compiler, Scan alg, ImmutableList<RexNode> filters, ImmutableIntList projects, ScannableTable scannableTable ) {
        final Enumerable<Row> rowEnumerable = Enumerables.toRow( scannableTable.scan( compiler.getDataContext() ) );
        return createEnumerable( compiler, alg, rowEnumerable, null, filters, projects );
    }


    private static ScanNode createQueryable( Compiler compiler, Scan alg, ImmutableList<RexNode> filters, ImmutableIntList projects, QueryableTable queryableTable ) {
        final DataContext root = compiler.getDataContext();
        final AlgOptTable algOptTable = alg.getTable();
        final Type elementType = queryableTable.getElementType();
        SchemaPlus schema = root.getRootSchema();
        for ( String name : Util.skipLast( algOptTable.getQualifiedName() ) ) {
            schema = schema.getSubSchema( name );
        }
        final Enumerable<Row> rowEnumerable;
        if ( elementType instanceof Class ) {
            //noinspection unchecked
            final Queryable<Object> queryable = Schemas.queryable( root, (Class) elementType, algOptTable.getQualifiedName() );
            ImmutableList.Builder<Field> fieldBuilder = ImmutableList.builder();
            Class type = (Class) elementType;
            for ( Field field : type.getFields() ) {
                if ( Modifier.isPublic( field.getModifiers() ) && !Modifier.isStatic( field.getModifiers() ) ) {
                    fieldBuilder.add( field );
                }
            }
            final List<Field> fields = fieldBuilder.build();
            rowEnumerable = queryable.select( o -> {
                final Object[] values = new Object[fields.size()];
                for ( int i = 0; i < fields.size(); i++ ) {
                    Field field = fields.get( i );
                    try {
                        values[i] = field.get( o );
                    } catch ( IllegalAccessException e ) {
                        throw new RuntimeException( e );
                    }
                }
                return new Row( values );
            } );
        } else {
            rowEnumerable = Schemas.queryable( root, Row.class, algOptTable.getQualifiedName() );
        }
        return createEnumerable( compiler, alg, rowEnumerable, null, filters, projects );
    }


    private static ScanNode createFilterable( Compiler compiler, Scan alg, ImmutableList<RexNode> filters, ImmutableIntList projects, FilterableTable filterableTable ) {
        final DataContext root = compiler.getDataContext();
        final List<RexNode> mutableFilters = Lists.newArrayList( filters );
        final Enumerable<Object[]> enumerable = filterableTable.scan( root, mutableFilters );
        for ( RexNode filter : mutableFilters ) {
            if ( !filters.contains( filter ) ) {
                throw RESOURCE.filterableTableInventedFilter( filter.toString() ).ex();
            }
        }
        final Enumerable<Row> rowEnumerable = Enumerables.toRow( enumerable );
        return createEnumerable( compiler, alg, rowEnumerable, null, mutableFilters, projects );
    }


    private static ScanNode createProjectableFilterable( Compiler compiler, Scan alg, ImmutableList<RexNode> filters, ImmutableIntList projects, ProjectableFilterableTable pfTable ) {
        final DataContext root = compiler.getDataContext();
        final ImmutableIntList originalProjects = projects;
        for ( ; ; ) {
            final List<RexNode> mutableFilters = Lists.newArrayList( filters );
            final int[] projectInts;
            if ( projects == null || projects.equals( Scan.identity( alg.getTable() ) ) ) {
                projectInts = null;
            } else {
                projectInts = projects.toIntArray();
            }
            final Enumerable<Object[]> enumerable1 = pfTable.scan( root, mutableFilters, projectInts );
            for ( RexNode filter : mutableFilters ) {
                if ( !filters.contains( filter ) ) {
                    throw RESOURCE.filterableTableInventedFilter( filter.toString() ).ex();
                }
            }
            final ImmutableBitSet usedFields = AlgOptUtil.InputFinder.bits( mutableFilters, null );
            if ( projects != null ) {
                int changeCount = 0;
                for ( int usedField : usedFields ) {
                    if ( !projects.contains( usedField ) ) {
                        // A field that is not projected is used in a filter that the table rejected. We won't be able to apply the filter later.
                        // Try again without any projects.
                        projects = ImmutableIntList.copyOf( Iterables.concat( projects, ImmutableList.of( usedField ) ) );
                        ++changeCount;
                    }
                }
                if ( changeCount > 0 ) {
                    continue;
                }
            }
            final Enumerable<Row> rowEnumerable = Enumerables.toRow( enumerable1 );
            final ImmutableIntList rejectedProjects;
            if ( Objects.equals( projects, originalProjects ) ) {
                rejectedProjects = null;
            } else {
                // We projected extra columns because they were needed in filters. Now project the leading columns.
                rejectedProjects = ImmutableIntList.identity( originalProjects.size() );
            }
            return createEnumerable( compiler, alg, rowEnumerable, projects, mutableFilters, rejectedProjects );
        }
    }


    private static ScanNode createEnumerable( Compiler compiler, Scan alg, Enumerable<Row> enumerable, final ImmutableIntList acceptedProjects, List<RexNode> rejectedFilters, final ImmutableIntList rejectedProjects ) {
        if ( !rejectedFilters.isEmpty() ) {
            final RexNode filter = RexUtil.composeConjunction( alg.getCluster().getRexBuilder(), rejectedFilters );
            // Re-map filter for the projects that have been applied already
            final RexNode filter2;
            final AlgDataType inputRowType;
            if ( acceptedProjects == null ) {
                filter2 = filter;
                inputRowType = alg.getRowType();
            } else {
                final Mapping mapping = Mappings.target( acceptedProjects, alg.getTable().getRowType().getFieldCount() );
                filter2 = RexUtil.apply( mapping, filter );
                final AlgDataTypeFactory.Builder builder = alg.getCluster().getTypeFactory().builder();
                final List<AlgDataTypeField> fieldList = alg.getTable().getRowType().getFieldList();
                for ( int acceptedProject : acceptedProjects ) {
                    builder.add( fieldList.get( acceptedProject ) );
                }
                inputRowType = builder.build();
            }
            final Scalar condition = compiler.compile( ImmutableList.of( filter2 ), inputRowType );
            final Context context = compiler.createContext();
            enumerable = enumerable.where( row -> {
                context.values = row.getValues();
                Boolean b = (Boolean) condition.execute( context );
                return b != null && b;
            } );
        }
        if ( rejectedProjects != null ) {
            enumerable = enumerable.select(
                    new Function1<Row, Row>() {
                        final Object[] values = new Object[rejectedProjects.size()];


                        @Override
                        public Row apply( Row row ) {
                            final Object[] inValues = row.getValues();
                            for ( int i = 0; i < rejectedProjects.size(); i++ ) {
                                values[i] = inValues[rejectedProjects.get( i )];
                            }
                            return Row.asCopy( values );
                        }
                    } );
        }
        return new ScanNode( compiler, alg, enumerable );
    }

}


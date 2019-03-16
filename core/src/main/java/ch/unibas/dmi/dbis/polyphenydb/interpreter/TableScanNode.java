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

package ch.unibas.dmi.dbis.polyphenydb.interpreter;


import static ch.unibas.dmi.dbis.polyphenydb.util.Static.RESOURCE;

import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexUtil;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Enumerables;
import ch.unibas.dmi.dbis.polyphenydb.schema.FilterableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.ProjectableFilterableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.QueryableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.ScannableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schemas;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableIntList;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.Mapping;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.Mappings;
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


/**
 * Interpreter node that implements a {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan}.
 */
public class TableScanNode implements Node {

    private TableScanNode( Compiler compiler, TableScan rel, Enumerable<Row> enumerable ) {
        compiler.enumerable( rel, enumerable );
    }


    public void run() {
        // nothing to do
    }


    /**
     * Creates a TableScanNode.
     *
     * Tries various table SPIs, and negotiates with the table which filters and projects it can implement. Adds to the Enumerable implementations of any filters and projects that cannot be implemented by the table.
     */
    static TableScanNode create( Compiler compiler, TableScan rel, ImmutableList<RexNode> filters, ImmutableIntList projects ) {
        final RelOptTable relOptTable = rel.getTable();
        final ProjectableFilterableTable pfTable = relOptTable.unwrap( ProjectableFilterableTable.class );
        if ( pfTable != null ) {
            return createProjectableFilterable( compiler, rel, filters, projects, pfTable );
        }
        final FilterableTable filterableTable = relOptTable.unwrap( FilterableTable.class );
        if ( filterableTable != null ) {
            return createFilterable( compiler, rel, filters, projects, filterableTable );
        }
        final ScannableTable scannableTable = relOptTable.unwrap( ScannableTable.class );
        if ( scannableTable != null ) {
            return createScannable( compiler, rel, filters, projects, scannableTable );
        }
        //noinspection unchecked
        final Enumerable<Row> enumerable = relOptTable.unwrap( Enumerable.class );
        if ( enumerable != null ) {
            return createEnumerable( compiler, rel, enumerable, null, filters, projects );
        }
        final QueryableTable queryableTable = relOptTable.unwrap( QueryableTable.class );
        if ( queryableTable != null ) {
            return createQueryable( compiler, rel, filters, projects, queryableTable );
        }
        throw new AssertionError( "cannot convert table " + relOptTable + " to enumerable" );
    }


    private static TableScanNode createScannable( Compiler compiler, TableScan rel, ImmutableList<RexNode> filters, ImmutableIntList projects, ScannableTable scannableTable ) {
        final Enumerable<Row> rowEnumerable = Enumerables.toRow( scannableTable.scan( compiler.getDataContext() ) );
        return createEnumerable( compiler, rel, rowEnumerable, null, filters, projects );
    }


    private static TableScanNode createQueryable( Compiler compiler, TableScan rel, ImmutableList<RexNode> filters, ImmutableIntList projects, QueryableTable queryableTable ) {
        final DataContext root = compiler.getDataContext();
        final RelOptTable relOptTable = rel.getTable();
        final Type elementType = queryableTable.getElementType();
        SchemaPlus schema = root.getRootSchema();
        for ( String name : Util.skipLast( relOptTable.getQualifiedName() ) ) {
            schema = schema.getSubSchema( name );
        }
        final Enumerable<Row> rowEnumerable;
        if ( elementType instanceof Class ) {
            //noinspection unchecked
            final Queryable<Object> queryable = Schemas.queryable( root, (Class) elementType, relOptTable.getQualifiedName() );
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
            rowEnumerable = Schemas.queryable( root, Row.class, relOptTable.getQualifiedName() );
        }
        return createEnumerable( compiler, rel, rowEnumerable, null, filters, projects );
    }


    private static TableScanNode createFilterable( Compiler compiler, TableScan rel, ImmutableList<RexNode> filters, ImmutableIntList projects, FilterableTable filterableTable ) {
        final DataContext root = compiler.getDataContext();
        final List<RexNode> mutableFilters = Lists.newArrayList( filters );
        final Enumerable<Object[]> enumerable = filterableTable.scan( root, mutableFilters );
        for ( RexNode filter : mutableFilters ) {
            if ( !filters.contains( filter ) ) {
                throw RESOURCE.filterableTableInventedFilter( filter.toString() ).ex();
            }
        }
        final Enumerable<Row> rowEnumerable = Enumerables.toRow( enumerable );
        return createEnumerable( compiler, rel, rowEnumerable, null, mutableFilters, projects );
    }


    private static TableScanNode createProjectableFilterable( Compiler compiler, TableScan rel, ImmutableList<RexNode> filters, ImmutableIntList projects, ProjectableFilterableTable pfTable ) {
        final DataContext root = compiler.getDataContext();
        final ImmutableIntList originalProjects = projects;
        for ( ; ; ) {
            final List<RexNode> mutableFilters = Lists.newArrayList( filters );
            final int[] projectInts;
            if ( projects == null || projects.equals( TableScan.identity( rel.getTable() ) ) ) {
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
            final ImmutableBitSet usedFields = RelOptUtil.InputFinder.bits( mutableFilters, null );
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
            return createEnumerable( compiler, rel, rowEnumerable, projects, mutableFilters, rejectedProjects );
        }
    }


    private static TableScanNode createEnumerable( Compiler compiler, TableScan rel, Enumerable<Row> enumerable, final ImmutableIntList acceptedProjects, List<RexNode> rejectedFilters, final ImmutableIntList rejectedProjects ) {
        if ( !rejectedFilters.isEmpty() ) {
            final RexNode filter = RexUtil.composeConjunction( rel.getCluster().getRexBuilder(), rejectedFilters );
            // Re-map filter for the projects that have been applied already
            final RexNode filter2;
            final RelDataType inputRowType;
            if ( acceptedProjects == null ) {
                filter2 = filter;
                inputRowType = rel.getRowType();
            } else {
                final Mapping mapping = Mappings.target( acceptedProjects, rel.getTable().getRowType().getFieldCount() );
                filter2 = RexUtil.apply( mapping, filter );
                final RelDataTypeFactory.Builder builder = rel.getCluster().getTypeFactory().builder();
                final List<RelDataTypeField> fieldList = rel.getTable().getRowType().getFieldList();
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
        return new TableScanNode( compiler, rel, enumerable );
    }
}


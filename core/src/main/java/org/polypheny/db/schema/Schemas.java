/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.catalogs.AdapterCatalog;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.interpreter.Row;
import org.polypheny.db.schema.types.FilterableEntity;
import org.polypheny.db.schema.types.ProjectableFilterableEntity;
import org.polypheny.db.schema.types.QueryableEntity;
import org.polypheny.db.schema.types.ScannableEntity;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;


/**
 * Utility functions for schemas.
 */
@Slf4j
@Deprecated
public final class Schemas {

    private Schemas() {
        throw new AssertionError( "no instances!" );
    }


    private static boolean canConvert( AlgDataType fromType, AlgDataType toType ) {
        return PolyTypeUtil.canAssignFrom( toType, fromType );
    }


    /**
     * Returns the expression for a schema.
     */
    public static Expression expression( AdapterCatalog snapshot ) {
        return snapshot.asExpression();
    }


    /**
     * Converts a schema expression to a given type by calling the {@link SchemaPlus#unwrap(Class)} method.
     */
    public static Expression unwrap( Expression call, Class<?> type ) {
        return Expressions.convert_( Expressions.call( call, BuiltInMethod.SCHEMA_PLUS_UNWRAP.method, Expressions.constant( type ) ), type );
    }


    public static DataContext createDataContext( Snapshot snapshot ) {
        return new DummyDataContext( snapshot );
    }


    /**
     * Returns a {@link Queryable}, given a fully-qualified table name.
     */
    public static Queryable<PolyValue[]> queryable( DataContext root, Class<PolyValue> clazz, String... names ) {
        return queryable( root, Arrays.asList( names ) );
    }


    /**
     * Returns a {@link Queryable}, given a fully-qualified table name as an iterable.
     */
    public static Queryable<PolyValue[]> queryable( DataContext root, Iterable<? extends String> names ) {
        Snapshot snapshot = root.getSnapshot();

        return queryable( root, snapshot, names.iterator().next() );

    }


    public static Enumerable<Row<PolyValue>> queryableRow( DataContext root, Class<Object> clazz, List<String> names ) {
        Snapshot snapshot = root.getSnapshot();

        throw new NotImplementedException();
    }


    /**
     * Returns a {@link Queryable}, given a schema and entity name.
     */
    public static Queryable<PolyValue[]> queryable( DataContext root, Snapshot snapshot, String entityName ) {
        LogicalTable table = snapshot.rel().getTable( null, entityName ).orElseThrow();
        return table.unwrap( QueryableEntity.class ).orElseThrow().asQueryable( root, snapshot );
    }


    /**
     * Returns an {@link org.apache.calcite.linq4j.Enumerable} over the rows of a given table, representing each row as an object array.
     */
    public static Enumerable<PolyValue[]> enumerable( final ScannableEntity table, final DataContext root ) {
        return table.scan( root );
    }


    /**
     * Returns an {@link org.apache.calcite.linq4j.Enumerable} over the rows of a given table, not applying any filters, representing each row as an object array.
     */
    public static Enumerable<PolyValue[]> enumerable( final FilterableEntity table, final DataContext root ) {
        return table.scan( root, ImmutableList.of() );
    }


    /**
     * Returns an {@link org.apache.calcite.linq4j.Enumerable} over the rows of a given table, not applying any filters and projecting all columns, representing each row as an object array.
     */
    public static Enumerable<PolyValue[]> enumerable( final ProjectableFilterableEntity table, final DataContext root ) {
        return table.scan( root, ImmutableList.of(), identity( table.getTupleType( root.getTypeFactory() ).getFieldCount() ) );
    }


    private static int[] identity( int count ) {
        final int[] integers = new int[count];
        for ( int i = 0; i < integers.length; i++ ) {
            integers[i] = i;
        }
        return integers;
    }


    /**
     * Returns an implementation of {@link AlgProtoDataType} that asks a given table for its row type with a given type factory.
     */
    public static AlgProtoDataType proto( final Entity entity ) {
        return entity::getTupleType;
    }


    /**
     * Returns an implementation of {@link AlgProtoDataType} that asks a given scalar function for its return type with a given type factory.
     */
    public static AlgProtoDataType proto( final ScalarFunction function ) {
        return function::getReturnType;
    }


    public static PathImpl path( ImmutableList<Pair<String, Namespace>> build ) {
        return new PathImpl( build );
    }


    /**
     * Returns the path to get to a schema from its root.
     */
    public static Path path( SchemaPlus schema ) {
        List<Pair<String, Namespace>> list = new ArrayList<>();
        for ( SchemaPlus s = schema; s != null; s = s.getParentSchema() ) {
            list.add( Pair.of( s.getName(), s ) );
        }
        return new PathImpl( ImmutableList.copyOf( Lists.reverse( list ) ) );
    }


    /**
     * Implementation of {@link Path}.
     */
    private static class PathImpl extends AbstractList<Pair<String, Namespace>> implements Path {

        private final ImmutableList<Pair<String, Namespace>> pairs;

        private static final PathImpl EMPTY = new PathImpl( ImmutableList.of() );


        PathImpl( ImmutableList<Pair<String, Namespace>> pairs ) {
            this.pairs = pairs;
        }


        @Override
        public boolean equals( Object o ) {
            return this == o
                    || o instanceof PathImpl
                    && pairs.equals( ((PathImpl) o).pairs );
        }


        @Override
        public int hashCode() {
            return pairs.hashCode();
        }


        @Override
        public Pair<String, Namespace> get( int index ) {
            return pairs.get( index );
        }


        @Override
        public int size() {
            return pairs.size();
        }


        @Override
        public Path parent() {
            if ( pairs.isEmpty() ) {
                throw new IllegalArgumentException( "at root" );
            }
            return new PathImpl( pairs.subList( 0, pairs.size() - 1 ) );
        }


        @Override
        public List<String> names() {
            return new AbstractList<String>() {
                @Override
                public String get( int index ) {
                    return pairs.get( index + 1 ).left;
                }


                @Override
                public int size() {
                    return pairs.size() - 1;
                }
            };
        }


        @Override
        public List<Namespace> schemas() {
            return Pair.right( pairs );
        }

    }

}

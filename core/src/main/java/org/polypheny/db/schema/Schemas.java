/*
 * Copyright 2019-2023 The Polypheny Project
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.lang.reflect.Type;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.config.PolyphenyDbConnectionConfig;
import org.polypheny.db.config.PolyphenyDbConnectionConfigImpl;
import org.polypheny.db.config.PolyphenyDbConnectionProperty;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.prepare.PolyphenyDbPrepare;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;


/**
 * Utility functions for schemas.
 */
public final class Schemas {

    private Schemas() {
        throw new AssertionError( "no instances!" );
    }


    private static boolean matches( AlgDataTypeFactory typeFactory, Function member, List<AlgDataType> argumentTypes ) {
        List<FunctionParameter> parameters = member.getParameters();
        if ( parameters.size() != argumentTypes.size() ) {
            return false;
        }
        for ( int i = 0; i < argumentTypes.size(); i++ ) {
            AlgDataType argumentType = argumentTypes.get( i );
            FunctionParameter parameter = parameters.get( i );
            if ( !canConvert( argumentType, parameter.getType( typeFactory ) ) ) {
                return false;
            }
        }
        return true;
    }


    private static boolean canConvert( AlgDataType fromType, AlgDataType toType ) {
        return PolyTypeUtil.canAssignFrom( toType, fromType );
    }


    /**
     * Returns the expression for a schema.
     */
    public static Expression expression( SchemaPlus schema ) {
        return schema.getExpression( schema.getParentSchema(), schema.getName() );
    }


    /**
     * Returns the expression for a sub-schema.
     */
    public static Expression subSchemaExpression( SchemaPlus schema, String name, Class type ) {
        // (Type) schemaExpression.getSubSchema("name")
        final Expression schemaExpression = expression( schema );
        Expression call =
                Expressions.call(
                        schemaExpression,
                        BuiltInMethod.SCHEMA_GET_SUB_SCHEMA.method,
                        Expressions.constant( name ) );
        //CHECKSTYLE: IGNORE 2
        //noinspection unchecked
        if ( false && type != null && !type.isAssignableFrom( Namespace.class ) ) {
            return unwrap( call, type );
        }
        return call;
    }


    /**
     * Converts a schema expression to a given type by calling the {@link SchemaPlus#unwrap(Class)} method.
     */
    public static Expression unwrap( Expression call, Class type ) {
        return Expressions.convert_( Expressions.call( call, BuiltInMethod.SCHEMA_PLUS_UNWRAP.method, Expressions.constant( type ) ), type );
    }


    /**
     * Returns the expression to access a table within a schema.
     */
    public static Expression tableExpression( SchemaPlus schema, Type elementType, String tableName, Class clazz ) {
        final MethodCallExpression expression;
        if ( Entity.class.isAssignableFrom( clazz ) ) {
            expression = Expressions.call(
                    expression( schema ),
                    BuiltInMethod.SCHEMA_GET_TABLE.method,
                    Expressions.constant( tableName ) );
            if ( ScannableEntity.class.isAssignableFrom( clazz ) ) {
                return Expressions.call(
                        BuiltInMethod.SCHEMAS_ENUMERABLE_SCANNABLE.method,
                        Expressions.convert_( expression, ScannableEntity.class ),
                        DataContext.ROOT );
            }
            if ( FilterableEntity.class.isAssignableFrom( clazz ) ) {
                return Expressions.call(
                        BuiltInMethod.SCHEMAS_ENUMERABLE_FILTERABLE.method,
                        Expressions.convert_( expression, FilterableEntity.class ),
                        DataContext.ROOT );
            }
            if ( ProjectableFilterableEntity.class.isAssignableFrom( clazz ) ) {
                return Expressions.call(
                        BuiltInMethod.SCHEMAS_ENUMERABLE_PROJECTABLE_FILTERABLE.method,
                        Expressions.convert_( expression, ProjectableFilterableEntity.class ),
                        DataContext.ROOT );
            }
        } else {
            expression = Expressions.call(
                    BuiltInMethod.SCHEMAS_QUERYABLE.method,
                    DataContext.ROOT,
                    expression( schema ),
                    Expressions.constant( elementType ),
                    Expressions.constant( tableName ) );
        }
        return Types.castIfNecessary( clazz, expression );
    }


    public static DataContext createDataContext( PolyphenyDbSchema rootSchema ) {
        return new DummyDataContext( rootSchema );
    }


    /**
     * Returns a {@link Queryable}, given a fully-qualified table name.
     */
    public static <E> Queryable<E> queryable( DataContext root, Class<E> clazz, String... names ) {
        return queryable( root, clazz, Arrays.asList( names ) );
    }


    /**
     * Returns a {@link Queryable}, given a fully-qualified table name as an iterable.
     */
    public static <E> Queryable<E> queryable( DataContext root, Class<E> clazz, Iterable<? extends String> names ) {
        PolyphenyDbSchema schema = root.getRootSchema();

        return queryable( root, schema, clazz, names.iterator().next() );

    }


    /**
     * Returns a {@link Queryable}, given a schema and table name.
     */
    public static <E> Queryable<E> queryable( DataContext root, PolyphenyDbSchema schema, Class<E> clazz, String tableName ) {
        //QueryableEntity table = (QueryableEntity) schema.getEntity( tableName );
        CatalogTable table = schema.getTable( List.of( tableName ) );
        return table.asQueryable( root, schema, tableName );
    }


    /**
     * Returns an {@link org.apache.calcite.linq4j.Enumerable} over the rows of a given table, representing each row as an object array.
     */
    public static Enumerable<Object[]> enumerable( final ScannableEntity table, final DataContext root ) {
        return table.scan( root );
    }


    /**
     * Returns an {@link org.apache.calcite.linq4j.Enumerable} over the rows of a given table, not applying any filters, representing each row as an object array.
     */
    public static Enumerable<Object[]> enumerable( final FilterableEntity table, final DataContext root ) {
        return table.scan( root, ImmutableList.of() );
    }


    /**
     * Returns an {@link org.apache.calcite.linq4j.Enumerable} over the rows of a given table, not applying any filters and projecting all columns, representing each row as an object array.
     */
    public static Enumerable<Object[]> enumerable( final ProjectableFilterableEntity table, final DataContext root ) {
        return table.scan( root, ImmutableList.of(), identity( table.getRowType( root.getTypeFactory() ).getFieldCount() ) );
    }


    private static int[] identity( int count ) {
        final int[] integers = new int[count];
        for ( int i = 0; i < integers.length; i++ ) {
            integers[i] = i;
        }
        return integers;
    }


    /**
     * Returns an {@link org.apache.calcite.linq4j.Enumerable} over object arrays, given a fully-qualified table name which leads to a {@link ScannableEntity}.
     */
    public static CatalogTable table( DataContext root, String... names ) {
        PolyphenyDbSchema schema = root.getRootSchema();
        return schema.getTable( List.of( names ) );
    }

    /**
     * Parses and validates a SQL query. For use within Polypheny-DB only.
     */
    /*public static ParseResult parse( final PolyphenyDbSchema schema, final List<String> schemaPath, final String sql ) {
        final PolyphenyDbPrepare prepare = PolyphenyDbPrepare.DEFAULT_FACTORY.apply();
        final ImmutableMap<PolyphenyDbConnectionProperty, String> propValues = ImmutableMap.of();
        final Context context = makeContext( schema, schemaPath, null, propValues );
        PolyphenyDbPrepare.Dummy.push( context );
        try {
            return prepare.parse( context, sql );
        } finally {
            PolyphenyDbPrepare.Dummy.pop( context );
        }
    }*/

    /**
     * Parses and validates a SQL query and converts to relational algebra. For use within Polypheny-DB only.
     */
    /*public static PolyphenyDbPrepare.ConvertResult convert( final PolyphenyDbSchema schema, final List<String> schemaPath, final String sql ) {
        final PolyphenyDbPrepare prepare = PolyphenyDbPrepare.DEFAULT_FACTORY.apply();
        final ImmutableMap<PolyphenyDbConnectionProperty, String> propValues = ImmutableMap.of();
        final Context context = makeContext( schema, schemaPath, null, propValues );
        PolyphenyDbPrepare.Dummy.push( context );
        try {
            return prepare.convert( context, sql );
        } finally {
            PolyphenyDbPrepare.Dummy.pop( context );
        }
    }*/


    /**
     * Creates a context for the purposes of preparing a statement.
     *
     * @param schema Schema
     * @param schemaPath Path wherein to look for functions
     * @param objectPath Path of the object being analyzed (usually a view), or null
     * @param propValues Connection properties
     * @return Context
     */
    private static Context makeContext( PolyphenyDbSchema schema, List<String> schemaPath, List<String> objectPath, final ImmutableMap<PolyphenyDbConnectionProperty, String> propValues ) {
        final Context context0 = PolyphenyDbPrepare.Dummy.peek();
        final PolyphenyDbConnectionConfig config = mutate( context0.config(), propValues );
        return makeContext( config, context0.getTypeFactory(), context0.getDataContext(), schema, schemaPath, objectPath );
    }


    private static PolyphenyDbConnectionConfig mutate( PolyphenyDbConnectionConfig config, ImmutableMap<PolyphenyDbConnectionProperty, String> propValues ) {
        for ( Map.Entry<PolyphenyDbConnectionProperty, String> e : propValues.entrySet() ) {
            config = ((PolyphenyDbConnectionConfigImpl) config).set( e.getKey(), e.getValue() );
        }
        return config;
    }


    private static Context makeContext(
            final PolyphenyDbConnectionConfig connectionConfig,
            final JavaTypeFactory typeFactory,
            final DataContext dataContext,
            final PolyphenyDbSchema schema,
            final List<String> schemaPath,
            final List<String> objectPath_ ) {
        final ImmutableList<String> objectPath = objectPath_ == null ? null : ImmutableList.copyOf( objectPath_ );
        return new Context() {
            @Override
            public JavaTypeFactory getTypeFactory() {
                return typeFactory;
            }


            @Override
            public PolyphenyDbSchema getRootSchema() {
                return schema;
            }


            @Override
            public String getDefaultSchemaName() {
                return null;
            }


            @Override
            public List<String> getDefaultSchemaPath() {
                // schemaPath is usually null. If specified, it overrides schema as the context within which the SQL is validated.
                return schemaPath;
            }


            @Override
            public List<String> getObjectPath() {
                return objectPath;
            }


            @Override
            public PolyphenyDbConnectionConfig config() {
                return connectionConfig;
            }


            @Override
            public DataContext getDataContext() {
                return dataContext;
            }


            @Override
            public Statement getStatement() {
                return null;
            }


            @Override
            public long getDatabaseId() {
                return 0;
            }


            @Override
            public int getCurrentUserId() {
                return 0;
            }

        };
    }


    /**
     * Returns an implementation of {@link AlgProtoDataType} that asks a given table for its row type with a given type factory.
     */
    public static AlgProtoDataType proto( final Entity entity ) {
        return entity::getRowType;
    }


    /**
     * Returns an implementation of {@link AlgProtoDataType} that asks a given scalar function for its return type with a given type factory.
     */
    public static AlgProtoDataType proto( final ScalarFunction function ) {
        return function::getReturnType;
    }


    /**
     * Returns a sub-schema of a given schema obtained by following a sequence of names.
     *
     * The result is null if the initial schema is null or any sub-schema does not exist.
     */
    public static PolyphenyDbSchema subSchema( PolyphenyDbSchema schema, Iterable<String> names ) {
        for ( String string : names ) {
            if ( schema == null ) {
                return null;
            }
        }
        return schema;
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
     * Dummy data context that has no variables.
     */
    private static class DummyDataContext implements DataContext {

        private final PolyphenyDbSchema rootSchema;
        private final ImmutableMap<String, Object> map;


        DummyDataContext( PolyphenyDbSchema rootSchema ) {
            this.rootSchema = rootSchema;
            this.map = ImmutableMap.of();
        }


        @Override
        public PolyphenyDbSchema getRootSchema() {
            return rootSchema;
        }


        @Override
        public JavaTypeFactory getTypeFactory() {
            //return connection.getTypeFactory();
            return new JavaTypeFactoryImpl(); // TODO MV: Potential bug
        }


        @Override
        public QueryProvider getQueryProvider() {
            return null; // TODO MV: potential bug
        }


        @Override
        public Object get( String name ) {
            return map.get( name );
        }


        @Override
        public void addAll( Map<String, Object> map ) {
            throw new UnsupportedOperationException( "This operation is not supported for " + getClass().getSimpleName() );
        }


        @Override
        public Statement getStatement() {
            return null;
        }


        @Override
        public void addParameterValues( long index, AlgDataType type, List<Object> data ) {
            throw new UnsupportedOperationException( "This operation is not supported for " + getClass().getSimpleName() );
        }


        @Override
        public AlgDataType getParameterType( long index ) {
            throw new UnsupportedOperationException( "This operation is not supported for " + getClass().getSimpleName() );
        }


        @Override
        public List<Map<Long, Object>> getParameterValues() {
            throw new UnsupportedOperationException( "This operation is not supported for " + getClass().getSimpleName() );
        }


        @Override
        public void setParameterValues( List<Map<Long, Object>> values ) {
            throw new UnsupportedOperationException( "This operation is not supported for " + getClass().getSimpleName() );
        }


        @Override
        public Map<Long, AlgDataType> getParameterTypes() {
            throw new UnsupportedOperationException( "This operation is not supported for " + getClass().getSimpleName() );
        }


        @Override
        public void setParameterTypes( Map<Long, AlgDataType> types ) {
            throw new UnsupportedOperationException( "This operation is not supported for " + getClass().getSimpleName() );
        }

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

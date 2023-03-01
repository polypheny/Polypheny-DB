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

package org.polypheny.db.adapter.java;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgReferentialConstraint;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.refactor.ScannableEntity;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.schema.Function;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.schema.Namespace.Schema;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.schema.Statistic;
import org.polypheny.db.schema.Statistics;
import org.polypheny.db.schema.TableMacro;
import org.polypheny.db.schema.TranslatableEntity;
import org.polypheny.db.schema.impl.AbstractNamespace;
import org.polypheny.db.schema.impl.ReflectiveFunctionBase;
import org.polypheny.db.util.BuiltInMethod;


/**
 * Implementation of {@link Namespace} that exposes the public fields and methods in a Java object.
 */
public class ReflectiveSchema extends AbstractNamespace implements Schema {

    private final Class clazz;
    private Object target;
    private Map<String, CatalogEntity> tableMap;
    private Multimap<String, Function> functionMap;


    /**
     * Creates a ReflectiveSchema.
     *
     * @param target Object whose fields will be sub-objects of the schema
     * @param id
     */
    public ReflectiveSchema( Object target, long id ) {
        super( id );
        this.clazz = target.getClass();
        this.target = target;
    }


    @Override
    public String toString() {
        return "ReflectiveSchema(target=" + target + ")";
    }


    /**
     * Returns the wrapped object.
     *
     * May not appear to be used, but is used in generated code via {@link org.polypheny.db.util.BuiltInMethod#REFLECTIVE_SCHEMA_GET_TARGET}.
     */
    public Object getTarget() {
        return target;
    }


    @Override
    public Map<String, CatalogEntity> getTables() {
        if ( tableMap == null ) {
            tableMap = createTableMap();
        }
        return tableMap;
    }


    private Map<String, CatalogEntity> createTableMap() {
        final ImmutableMap.Builder<String, CatalogEntity> builder = ImmutableMap.builder();
        for ( Field field : clazz.getFields() ) {
            final String fieldName = field.getName();
            final CatalogEntity entity = fieldRelation( field );
            if ( entity == null ) {
                continue;
            }
            builder.put( fieldName, entity );
        }
        Map<String, CatalogEntity> tableMap = builder.build();
        // Unique-Key - Foreign-Key
        for ( Field field : clazz.getFields() ) {
            if ( AlgReferentialConstraint.class.isAssignableFrom( field.getType() ) ) {
                AlgReferentialConstraint rc;
                try {
                    rc = (AlgReferentialConstraint) field.get( target );
                } catch ( IllegalAccessException e ) {
                    throw new RuntimeException( "Error while accessing field " + field, e );
                }
                // CatalogEntity table = (FieldEntity) tableMap.get( Util.last( rc.getSourceQualifiedName() ) );
                // assert table != null;
                // table.statistic = Statistics.of( ImmutableList.copyOf( Iterables.concat( table.getStatistic().getReferentialConstraints(), Collections.singleton( rc ) ) ) );
                // todo dl;
            }
        }
        return tableMap;
    }


    @Override
    protected Multimap<String, Function> getFunctionMultimap() {
        if ( functionMap == null ) {
            functionMap = createFunctionMap();
        }
        return functionMap;
    }


    private Multimap<String, Function> createFunctionMap() {
        final ImmutableMultimap.Builder<String, Function> builder = ImmutableMultimap.builder();
        for ( Method method : clazz.getMethods() ) {
            final String methodName = method.getName();
            if ( method.getDeclaringClass() == Object.class || methodName.equals( "toString" ) ) {
                continue;
            }
            if ( TranslatableEntity.class.isAssignableFrom( method.getReturnType() ) ) {
                final TableMacro tableMacro = new MethodTableMacro( this, method );
                builder.put( methodName, tableMacro );
            }
        }
        return builder.build();
    }


    /**
     * Returns an expression for the object wrapped by this schema (not the schema itself).
     */
    Expression getTargetExpression( Snapshot snapshot, String name ) {
        return Types.castIfNecessary(
                target.getClass(),
                Expressions.call(
                        Schemas.unwrap( getExpression( snapshot, name ), ReflectiveSchema.class ),
                        BuiltInMethod.REFLECTIVE_SCHEMA_GET_TARGET.method ) );
    }


    /**
     * Returns a table based on a particular field of this schema. If the field is not of the right type to be a relation, returns null.
     */
    private <T> CatalogEntity fieldRelation( final Field field ) {
        final Type elementType = getElementType( field.getType() );
        if ( elementType == null ) {
            return null;
        }
        Object o;
        try {
            o = field.get( target );
        } catch ( IllegalAccessException e ) {
            throw new RuntimeException( "Error while accessing field " + field, e );
        }
        final Enumerable<T> enumerable = (Enumerable<T>) toEnumerable( o );
        return null;
        // return new FieldEntity<>( field, elementType, enumerable, null, null, null ); todo dl
    }


    /**
     * Deduces the element type of a collection; same logic as {@link #toEnumerable}
     */
    private static Type getElementType( Class<?> clazz ) {
        if ( clazz.isArray() ) {
            return clazz.getComponentType();
        }
        if ( Iterable.class.isAssignableFrom( clazz ) ) {
            return Object.class;
        }
        return null; // not a collection/array/iterable
    }


    private static Enumerable<?> toEnumerable( final Object o ) {
        if ( o.getClass().isArray() ) {
            if ( o instanceof Object[] ) {
                return Linq4j.asEnumerable( (Object[]) o );
            } else {
                return Linq4j.asEnumerable( Primitive.asList( o ) );
            }
        }
        if ( o instanceof Iterable ) {
            return Linq4j.asEnumerable( (Iterable) o );
        }
        throw new RuntimeException( "Cannot convert " + o.getClass() + " into a Enumerable" );
    }


    /**
     * Table that is implemented by reading from a Java object.
     */
    private static class ReflectiveEntity extends LogicalTable implements ScannableEntity {

        private final Type elementType;
        private final Enumerable enumerable;


        ReflectiveEntity( Type elementType, Enumerable<?> enumerable, Long id, Long partitionId, Long adapterId ) {
            //super( elementType, id, partitionId, adapterId );
            super( id, "test", null, -1, -1, -1, EntityType.ENTITY, null, ImmutableList.of(), false, null );
            this.elementType = elementType;
            this.enumerable = enumerable;
        }


        @Override
        public Statistic getStatistic() {
            return Statistics.UNKNOWN;
        }


        @Override
        public Enumerable<Object[]> scan( DataContext root ) {
            if ( elementType == Object[].class ) {
                //noinspection unchecked
                return enumerable;
            } else {
                //noinspection unchecked
                return enumerable.select( new FieldSelector( (Class<?>) elementType ) );
            }
        }


        /*@Override
        public <T> Queryable<T> asQueryable( DataContext dataContext, Snapshot snapshot, String tableName ) {
            return new AbstractTableQueryable<T>( dataContext, snapshot, this, tableName ) {
                @Override
                @SuppressWarnings("unchecked")
                public Enumerator<T> enumerator() {
                    return (Enumerator<T>) enumerable.enumerator();
                }
            };
        }*/

    }


    /**
     * Table macro based on a Java method.
     */
    private static class MethodTableMacro extends ReflectiveFunctionBase implements TableMacro {

        private final ReflectiveSchema schema;


        MethodTableMacro( ReflectiveSchema schema, Method method ) {
            super( method );
            this.schema = schema;
            assert TranslatableEntity.class.isAssignableFrom( method.getReturnType() ) : "Method should return TranslatableTable so the macro can be expanded";
        }


        public String toString() {
            return "Member {method=" + method + "}";
        }


        @Override
        public TranslatableEntity apply( final List<Object> arguments ) {
            try {
                final Object o = method.invoke( schema.getTarget(), arguments.toArray() );
                return (TranslatableEntity) o;
            } catch ( IllegalAccessException | InvocationTargetException e ) {
                throw new RuntimeException( e );
            }
        }

    }


    /**
     * Table based on a Java field.
     *
     * @param <T> element type
     */
    private static class FieldEntity<T> extends ReflectiveEntity {

        private final Field field;
        private Statistic statistic;


        FieldEntity( Field field, Type elementType, Enumerable<T> enumerable, Long id, Long partitionId, Long adapterId ) {
            this( field, elementType, enumerable, Statistics.UNKNOWN, id, partitionId, adapterId );
        }


        FieldEntity( Field field, Type elementType, Enumerable<T> enumerable, Statistic statistic, Long id, Long partitionId, Long adapterId ) {
            super( elementType, enumerable, id, partitionId, adapterId );
            this.field = field;
            this.statistic = statistic;
        }


        public String toString() {
            return "Relation {field=" + field.getName() + "}";
        }


        @Override
        public Statistic getStatistic() {
            return statistic;
        }


    }


    /**
     * Function that returns an array of a given object's field values.
     */
    private static class FieldSelector implements Function1<Object, Object[]> {

        private final Field[] fields;


        FieldSelector( Class elementType ) {
            this.fields = elementType.getFields();
        }


        @Override
        public Object[] apply( Object o ) {
            try {
                final Object[] objects = new Object[fields.length];
                for ( int i = 0; i < fields.length; i++ ) {
                    objects[i] = fields[i].get( o );
                }
                return objects;
            } catch ( IllegalAccessException e ) {
                throw new RuntimeException( e );
            }
        }

    }

}


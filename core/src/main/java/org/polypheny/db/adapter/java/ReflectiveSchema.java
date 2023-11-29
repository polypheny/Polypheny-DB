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
import org.apache.calcite.linq4j.tree.Primitive;
import org.polypheny.db.algebra.AlgReferentialConstraint;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.schema.Function;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.schema.Namespace.Schema;
import org.polypheny.db.schema.TableMacro;
import org.polypheny.db.schema.impl.AbstractNamespace;
import org.polypheny.db.schema.impl.ReflectiveFunctionBase;
import org.polypheny.db.schema.types.TranslatableEntity;


/**
 * Implementation of {@link Namespace} that exposes the public fields and methods in a Java object.
 */
public class ReflectiveSchema extends AbstractNamespace implements Schema {

    private final Class clazz;
    private Object target;
    private Map<String, Entity> tableMap;
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
    public Map<String, Entity> getTables() {
        if ( tableMap == null ) {
            tableMap = createTableMap();
        }
        return tableMap;
    }


    private Map<String, Entity> createTableMap() {
        final ImmutableMap.Builder<String, Entity> builder = ImmutableMap.builder();
        for ( Field field : clazz.getFields() ) {
            final String fieldName = field.getName();
            final Entity entity = fieldRelation( field );
            if ( entity == null ) {
                continue;
            }
            builder.put( fieldName, entity );
        }
        Map<String, Entity> tableMap = builder.build();
        // Unique-Key - Foreign-Key
        for ( Field field : clazz.getFields() ) {
            if ( AlgReferentialConstraint.class.isAssignableFrom( field.getType() ) ) {
                AlgReferentialConstraint rc;
                try {
                    rc = (AlgReferentialConstraint) field.get( target );
                } catch ( IllegalAccessException e ) {
                    throw new GenericRuntimeException( "Error while accessing field " + field, e );
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
     * Returns a table based on a particular field of this schema. If the field is not of the right type to be a relation, returns null.
     */
    private <T> Entity fieldRelation( final Field field ) {
        final Type elementType = getElementType( field.getType() );
        if ( elementType == null ) {
            return null;
        }
        Object o;
        try {
            o = field.get( target );
        } catch ( IllegalAccessException e ) {
            throw new GenericRuntimeException( "Error while accessing field " + field, e );
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


}


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

package org.polypheny.db.algebra.enumerable;


import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.interpreter.Row;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.volcano.VolcanoCost;
import org.polypheny.db.schema.types.FilterableEntity;
import org.polypheny.db.schema.types.ProjectableFilterableEntity;
import org.polypheny.db.schema.types.QueryableEntity;
import org.polypheny.db.schema.types.ScannableEntity;
import org.polypheny.db.schema.types.StreamableEntity;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.RunMode;


/**
 * Implementation of {@link RelScan} in {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableScan extends RelScan<PhysicalTable> implements EnumerableAlg {

    private final Class<?> elementType;


    /**
     * Creates an EnumerableScan.
     * <p>
     * Use {@link #create} unless you know what you are doing.
     */
    public EnumerableScan( AlgCluster cluster, AlgTraitSet traitSet, PhysicalTable table, Class<?> elementType ) {
        super( cluster, traitSet, table );
        assert getConvention() instanceof EnumerableConvention;
        this.elementType = elementType;
    }


    /**
     * Creates an EnumerableScan.
     */
    public static EnumerableScan create( AlgCluster cluster, Entity entity ) {
        PhysicalTable oPhysicalTable = entity.unwrap( PhysicalTable.class ).orElseThrow();
        Class<?> elementType = EnumerableScan.deduceElementType( oPhysicalTable );
        final AlgTraitSet traitSet =
                cluster.traitSetOf( EnumerableConvention.INSTANCE )
                        .replaceIfs( AlgCollationTraitDef.INSTANCE, entity::getCollations );
        return new EnumerableScan( cluster, traitSet, oPhysicalTable, elementType );
    }


    @Override
    public boolean equals( Object obj ) {
        return obj == this
                || obj instanceof EnumerableScan
                && entity.id == ((EnumerableScan) obj).getEntity().id;
    }


    @Override
    public int hashCode() {
        return entity.hashCode();
    }


    /**
     * Returns whether EnumerableScan can generate code to handle a particular variant of the Table SPI.
     */
    public static boolean canHandle( Entity entity ) {
        // FilterableTable and ProjectableFilterableTable cannot be handled in/ enumerable convention because they might reject filters and those filters would need to be handled dynamically.
        return entity instanceof QueryableEntity || entity instanceof ScannableEntity;
    }


    public static Class<?> deduceElementType( PhysicalTable entity ) {
        if ( entity instanceof QueryableEntity queryableTable ) {
            final Type type = queryableTable.getElementType();
            if ( type instanceof Class ) {
                return (Class<?>) type;
            } else {
                return Object[].class;
            }
        } else if ( entity instanceof ScannableEntity
                || entity instanceof FilterableEntity
                || entity instanceof ProjectableFilterableEntity
                || entity instanceof StreamableEntity ) {
            return Object[].class;
        } else {
            return Object.class;
        }
    }


    private Expression getExpression( PhysType physType ) {
        final Expression expression = entity.asExpression();
        final Expression expression2 = toEnumerable( expression );
        assert Types.isAssignableFrom( Enumerable.class, expression2.getType() );
        return toRows( physType, expression2 );
    }


    private Expression toEnumerable( Expression expression ) {
        final Type type = expression.getType();
        if ( Types.isArray( type ) ) {
            if ( Types.toClass( type ).getComponentType().isPrimitive() ) {
                expression = Expressions.call( BuiltInMethod.AS_LIST.method, expression );
            }
            return Expressions.call( BuiltInMethod.AS_ENUMERABLE.method, expression );
        } else if ( Types.isAssignableFrom( Iterable.class, type ) && !Types.isAssignableFrom( Enumerable.class, type ) ) {
            return Expressions.call( BuiltInMethod.AS_ENUMERABLE2.method, expression );
        } else if ( Types.isAssignableFrom( Queryable.class, type ) ) {
            // Queryable extends Enumerable, but it's too "clever", so we call Queryable.asEnumerable so that operations such as take(int) will be evaluated directly.
            return Expressions.call( expression, BuiltInMethod.QUERYABLE_AS_ENUMERABLE.method );
        }
        return expression;
    }


    private Expression toRows( PhysType physType, Expression expression ) {
        if ( physType.getFormat() == JavaTupleFormat.SCALAR
                && Object[].class.isAssignableFrom( elementType )
                && getTupleType().getFieldCount() == 1
                && (entity.unwrap( ScannableEntity.class ).isPresent()
                || entity.unwrap( FilterableEntity.class ).isPresent()
                || entity.unwrap( ProjectableFilterableEntity.class ).isPresent()) ) {
            return Expressions.call( BuiltInMethod.SLICE0.method, expression );
        }
        JavaTupleFormat oldFormat = format();
        if ( physType.getFormat() == oldFormat && !hasCollectionField( rowType ) ) {
            return expression;
        }
        final ParameterExpression row_ = Expressions.parameter( elementType, "row" );
        final int fieldCount = entity.getTupleType().getFieldCount();
        List<Expression> expressionList = new ArrayList<>( fieldCount );
        for ( int i = 0; i < fieldCount; i++ ) {
            expressionList.add( fieldExpression( row_, i, physType, oldFormat ) );
        }
        return Expressions.call(
                expression,
                BuiltInMethod.SELECT.method,
                Expressions.lambda( Function1.class, physType.record( expressionList ), row_ ) );
    }


    private Expression fieldExpression( ParameterExpression row_, int i, PhysType physType, JavaTupleFormat format ) {
        final Expression e = format.field( row_, i, null, physType.getJavaFieldType( i ) );
        final AlgDataType algFieldType = physType.getTupleType().getFields().get( i ).getType();
        switch ( algFieldType.getPolyType() ) {
            case ARRAY:
            case MULTISET:
                // We can't represent a multiset or array as a List<Employee>, because the consumer does not know the element type.
                // The standard element type is List. We need to convert to a List<List>.
                final JavaTypeFactory typeFactory = (JavaTypeFactory) getCluster().getTypeFactory();
                final PhysType elementPhysType = PhysTypeImpl.of( typeFactory, algFieldType.getComponentType(), JavaTupleFormat.CUSTOM );
                final MethodCallExpression e2 = Expressions.call( BuiltInMethod.AS_ENUMERABLE2.method, e );
                final AlgDataType dummyType = this.rowType;
                final Expression e3 = elementPhysType.convertTo( e2, PhysTypeImpl.of( typeFactory, dummyType, JavaTupleFormat.LIST ) );
                return Expressions.call( e3, BuiltInMethod.ENUMERABLE_TO_LIST.method );
            default:
                return e;
        }
    }


    private JavaTupleFormat format() {
        int fieldCount = getTupleType().getFieldCount();
        if ( fieldCount == 0 ) {
            return JavaTupleFormat.LIST;
        }
        if ( Object[].class.isAssignableFrom( elementType ) ) {
            return fieldCount == 1 ? JavaTupleFormat.SCALAR : JavaTupleFormat.ARRAY;
        }
        if ( Row.class.isAssignableFrom( elementType ) ) {
            return JavaTupleFormat.ROW;
        }
        if ( fieldCount == 1 && (Object.class == elementType
                || Primitive.is( elementType )
                || Number.class.isAssignableFrom( elementType )) ) {
            return JavaTupleFormat.SCALAR;
        }
        return JavaTupleFormat.CUSTOM;
    }


    private boolean hasCollectionField( AlgDataType rowType ) {
        for ( AlgDataTypeField field : rowType.getFields() ) {
            switch ( field.getType().getPolyType() ) {
                case ARRAY:
                case MULTISET:
                    return true;
            }
        }
        return false;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new EnumerableScan( getCluster(), traitSet, entity, elementType );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        // Note that representation is ARRAY. This assumes that the table returns a Object[] for each record. Actually a Table<T> can return any type T. And, if it is a JdbcTable, we'd like to be
        // able to generate alternate accessors that return e.g. synthetic records {T0 f0; T1 f1; ...} and don't box every primitive value.
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getTupleType(),
                        format() );
        final Expression expression = getExpression( physType );
        return implementor.result( physType, Blocks.toBlock( expression ) );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        if ( Catalog.mode == RunMode.TEST ) {
            // normally this enumerable is not used by Polypheny and is therefore "removed" by an infinite cost,
            // but theoretically it is able to handle scans on the application layer
            // this is tested by different instances and should then lead to a finite selfCost
            return super.computeSelfCost( planner, mq );
        }
        return VolcanoCost.FACTORY.makeInfiniteCost();
    }

}


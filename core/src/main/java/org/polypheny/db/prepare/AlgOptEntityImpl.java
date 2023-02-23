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

package org.polypheny.db.prepare;


import com.google.common.collect.ImmutableList;
import java.util.AbstractList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.StatisticsManager;
import org.polypheny.db.adapter.enumerable.EnumerableScan;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgDistribution;
import org.polypheny.db.algebra.AlgDistributionTraitDef;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgReferentialConstraint;
import org.polypheny.db.algebra.constant.Modality;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptEntity;
import org.polypheny.db.plan.AlgOptSchema;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.Prepare.AbstractPreparingEntity;
import org.polypheny.db.runtime.Hook;
import org.polypheny.db.schema.ColumnStrategy;
import org.polypheny.db.schema.Entity;
import org.polypheny.db.schema.FilterableEntity;
import org.polypheny.db.schema.ModifiableEntity;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.ProjectableFilterableEntity;
import org.polypheny.db.schema.QueryableEntity;
import org.polypheny.db.schema.ScannableEntity;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.schema.StreamableEntity;
import org.polypheny.db.schema.TranslatableEntity;
import org.polypheny.db.schema.Wrapper;
import org.polypheny.db.util.AccessType;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.InitializerExpressionFactory;
import org.polypheny.db.util.NullInitializerExpressionFactory;
import org.polypheny.db.util.Util;


/**
 * Implementation of {@link AlgOptEntity}.
 */
public class AlgOptEntityImpl extends AbstractPreparingEntity {

    private final transient AlgOptSchema schema;
    private final AlgDataType rowType;
    @Getter
    @Nullable
    private final Entity entity;

    @Getter
    @Nullable
    private final CatalogEntity catalogEntity;

    /**
     * Estimate for the row count, or null.
     * <p>
     * If not null, overrides the estimate from the actual table.
     */
    private final Double rowCount;
    @Getter
    @Nullable
    private final CatalogPartitionPlacement partitionPlacement;


    private AlgOptEntityImpl(
            AlgOptSchema schema,
            AlgDataType rowType,
            @Nullable Entity entity,
            @Nullable CatalogEntity catalogEntity,
            @Nullable CatalogPartitionPlacement placement,
            @Nullable Double rowCount ) {
        this.schema = schema;
        this.rowType = Objects.requireNonNull( rowType );
        this.entity = entity;
        this.partitionPlacement = placement;
        this.catalogEntity = catalogEntity;
        this.rowCount = rowCount;
    }


    public static AlgOptEntityImpl create( AlgOptSchema schema, AlgDataType rowType ) {
        return new AlgOptEntityImpl( schema, rowType, null, null, null, null );
    }


    public static AlgOptEntityImpl create( AlgOptSchema schema, AlgDataType rowType, CatalogEntity catalogEntity, CatalogPartitionPlacement placement, Double count ) {
        Double rowCount;
        if ( count == null ) {
            rowCount = Double.valueOf( StatisticsManager.getInstance().rowCountPerTable( catalogEntity.id ) );
        } else {
            rowCount = count;
        }

        return new AlgOptEntityImpl( schema, rowType, null, catalogEntity, placement, rowCount );
    }


    /**
     * Creates a copy of this RelOptTable. The new RelOptTable will have newRowType.
     */
    public AlgOptEntityImpl copy( AlgDataType newRowType ) {
        return new AlgOptEntityImpl( this.schema, newRowType, this.entity, this.catalogEntity, this.partitionPlacement, this.rowCount );
    }


    public static AlgOptEntityImpl create( AlgOptSchema schema, AlgDataType rowType, Entity entity, CatalogEntity catalogEntity, CatalogPartitionPlacement placement ) {
        assert entity instanceof TranslatableEntity
                || entity instanceof ScannableEntity
                || entity instanceof ModifiableEntity;
        return new AlgOptEntityImpl( schema, rowType, entity, catalogEntity, placement, null );
    }


    @Override
    public <T> T unwrap( Class<T> clazz ) {
        if ( clazz.isInstance( this ) ) {
            return clazz.cast( this );
        }
        if ( clazz.isInstance( entity ) ) {
            return clazz.cast( entity );
        }
        if ( entity instanceof Wrapper ) {
            final T t = ((Wrapper) entity).unwrap( clazz );
            if ( t != null ) {
                return t;
            }
        }
        if ( clazz == PolyphenyDbSchema.class ) {
            return clazz.cast( Schemas.subSchema( ((PolyphenyDbCatalogReader) schema).rootSchema, List.of( catalogEntity.unwrap( CatalogTable.class ).getNamespaceName(), catalogEntity.name ) ) );
        }
        return null;
    }


    @Override
    public Expression getExpression( Class<?> clazz ) {
        if ( partitionPlacement != null ) {
            return Expressions.call(
                    Expressions.call( Catalog.class, "getInstance" ),
                    "getPartitionPlacement",
                    Expressions.constant( partitionPlacement.adapterId ),
                    Expressions.constant( partitionPlacement.partitionId ) );
        } else if ( catalogEntity != null ) {
            return Expressions.call(
                    Expressions.call( Catalog.class, "getInstance" ),
                    "getTable",
                    Expressions.constant( catalogEntity.id ) );
        }

        return null;
    }


    @Override
    protected AlgOptEntity extend( Entity extendedEntity ) {
        final AlgDataType extendedRowType = extendedEntity.getRowType( AlgDataTypeFactory.DEFAULT );
        return new AlgOptEntityImpl(
                getRelOptSchema(),
                extendedRowType,
                extendedEntity,
                null,
                null,
                expressionFunction,
                getRowCount() );
    }


    @Override
    public boolean equals( Object obj ) {
        return obj instanceof AlgOptEntityImpl
                && this.rowType.equals( ((AlgOptEntityImpl) obj).getRowType() )
                && this.entity == ((AlgOptEntityImpl) obj).entity;
    }


    @Override
    public int hashCode() {
        return (this.entity == null) ? super.hashCode() : this.entity.hashCode();
    }


    @Override
    public double getRowCount() {
        if ( rowCount != null ) {
            return rowCount;
        }
        if ( entity != null ) {
            final Double rowCount = entity.getStatistic().getRowCount();
            if ( rowCount != null ) {
                return rowCount;
            }
        }
        return 100d;
    }


    @Override
    public AlgOptSchema getRelOptSchema() {
        return schema;
    }


    @Override
    public AlgNode toAlg( ToAlgContext context, AlgTraitSet traitSet ) {
        // Make sure rowType's list is immutable. If rowType is DynamicRecordType, creates a new RelOptTable by replacing with
        // immutable RelRecordType using the same field list.
        if ( this.getRowType().isDynamicStruct() ) {
            final AlgDataType staticRowType = new AlgRecordType( getRowType().getFieldList() );
            final AlgOptEntity algOptEntity = this.copy( staticRowType );
            return algOptEntity.toAlg( context, traitSet );
        }

        // If there are any virtual columns, create a copy of this table without those virtual columns.
        final List<ColumnStrategy> strategies = getColumnStrategies();
        if ( strategies.contains( ColumnStrategy.VIRTUAL ) ) {
            final AlgDataTypeFactory.Builder b = context.getCluster().getTypeFactory().builder();
            for ( AlgDataTypeField field : rowType.getFieldList() ) {
                if ( strategies.get( field.getIndex() ) != ColumnStrategy.VIRTUAL ) {
                    b.add( field.getName(), null, field.getType() );
                }
            }
            final AlgOptEntity algOptEntity =
                    new AlgOptEntityImpl( this.schema, b.build(), this.entity, this.catalogEntity, this.partitionPlacement, this.expressionFunction, this.rowCount ) {
                        @Override
                        public <T> T unwrap( Class<T> clazz ) {
                            if ( clazz.isAssignableFrom( InitializerExpressionFactory.class ) ) {
                                return clazz.cast( NullInitializerExpressionFactory.INSTANCE );
                            }
                            return super.unwrap( clazz );
                        }
                    };
            return algOptEntity.toAlg( context, traitSet );
        }

        if ( entity instanceof TranslatableEntity ) {
            return ((TranslatableEntity) entity).toAlg( context, this, traitSet );
        }
        final AlgOptCluster cluster = context.getCluster();
        if ( Hook.ENABLE_BINDABLE.get( false ) ) {
            return LogicalRelScan.create( cluster, this );
        }
        if ( PolyphenyDbPrepareImpl.ENABLE_ENUMERABLE && entity instanceof QueryableEntity ) {
            return EnumerableScan.create( cluster, this );
        }
        if ( entity instanceof ScannableEntity
                || entity instanceof FilterableEntity
                || entity instanceof ProjectableFilterableEntity ) {
            return LogicalRelScan.create( cluster, this );
        }
        if ( PolyphenyDbPrepareImpl.ENABLE_ENUMERABLE ) {
            return EnumerableScan.create( cluster, this );
        }
        throw new AssertionError();
    }


    @Override
    public List<AlgCollation> getCollationList() {
        if ( entity != null ) {
            return entity.getStatistic().getCollations();
        }
        return ImmutableList.of();
    }


    @Override
    public AlgDistribution getDistribution() {
        if ( entity != null ) {
            return entity.getStatistic().getDistribution();
        }
        return AlgDistributionTraitDef.INSTANCE.getDefault();
    }


    @Override
    public boolean isKey( ImmutableBitSet columns ) {
        if ( entity != null ) {
            return entity.getStatistic().isKey( columns );
        }
        return false;
    }


    @Override
    public List<AlgReferentialConstraint> getReferentialConstraints() {
        if ( entity != null ) {
            return entity.getStatistic().getReferentialConstraints();
        }
        return ImmutableList.of();
    }


    @Override
    public AlgDataType getRowType() {
        return rowType;
    }


    @Override
    public boolean supportsModality( Modality modality ) {
        switch ( modality ) {
            case STREAM:
                return entity instanceof StreamableEntity;
            default:
                return !(entity instanceof StreamableEntity);
        }
    }


    @Override
    public List<String> getQualifiedName() {
        return List.of( catalogEntity.unwrap( CatalogTable.class ).getNamespaceName(), catalogEntity.name );
    }


    @Override
    public Monotonicity getMonotonicity( String columnName ) {
        for ( AlgCollation collation : entity.getStatistic().getCollations() ) {
            final AlgFieldCollation fieldCollation = collation.getFieldCollations().get( 0 );
            final int fieldIndex = fieldCollation.getFieldIndex();
            if ( fieldIndex < rowType.getFieldCount() && rowType.getFieldNames().get( fieldIndex ).equals( columnName ) ) {
                return fieldCollation.direction.monotonicity();
            }
        }
        return Monotonicity.NOT_MONOTONIC;
    }


    @Override
    public AccessType getAllowedAccess() {
        return AccessType.ALL;
    }


    /**
     * Helper for {@link #getColumnStrategies()}.
     */
    public static List<ColumnStrategy> columnStrategies( final AlgOptEntity table ) {
        final int fieldCount = table.getRowType().getFieldCount();
        final InitializerExpressionFactory ief = Util.first(
                table.unwrap( InitializerExpressionFactory.class ),
                NullInitializerExpressionFactory.INSTANCE );
        return new AbstractList<>() {
            @Override
            public int size() {
                return fieldCount;
            }


            @Override
            public ColumnStrategy get( int index ) {
                return ief.generationStrategy( table, index );
            }
        };
    }


    /**
     * Converts the ordinal of a field into the ordinal of a stored field.
     * That is, it subtracts the number of virtual fields that come before it.
     */
    public static int realOrdinal( final AlgOptEntity table, int i ) {
        List<ColumnStrategy> strategies = table.getColumnStrategies();
        int n = 0;
        for ( int j = 0; j < i; j++ ) {
            switch ( strategies.get( j ) ) {
                case VIRTUAL:
                    ++n;
            }
        }
        return i - n;
    }


    /**
     * Returns the row type of a table after any {@link ColumnStrategy#VIRTUAL} columns have been removed. This is the type
     * of the records that are actually stored.
     */
    public static AlgDataType realRowType( AlgOptEntity table ) {
        final AlgDataType rowType = table.getRowType();
        final List<ColumnStrategy> strategies = columnStrategies( table );
        if ( !strategies.contains( ColumnStrategy.VIRTUAL ) ) {
            return rowType;
        }
        final AlgDataTypeFactory.Builder builder = AlgDataTypeFactoryImpl.DEFAULT.builder();
        for ( AlgDataTypeField field : rowType.getFieldList() ) {
            if ( strategies.get( field.getIndex() ) != ColumnStrategy.VIRTUAL ) {
                builder.add( field );
            }
        }
        return builder.build();
    }

}


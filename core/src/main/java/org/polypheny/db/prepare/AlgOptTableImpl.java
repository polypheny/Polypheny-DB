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
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.enumerable.EnumerableScan;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgDistribution;
import org.polypheny.db.algebra.AlgDistributionTraitDef;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgReferentialConstraint;
import org.polypheny.db.algebra.constant.Modality;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.logical.relational.LogicalScan;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptSchema;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.runtime.Hook;
import org.polypheny.db.schema.ColumnStrategy;
import org.polypheny.db.schema.FilterableTable;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.ProjectableFilterableTable;
import org.polypheny.db.schema.QueryableTable;
import org.polypheny.db.schema.ScannableTable;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.schema.StreamableTable;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.TranslatableTable;
import org.polypheny.db.schema.Wrapper;
import org.polypheny.db.util.AccessType;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.InitializerExpressionFactory;
import org.polypheny.db.util.NullInitializerExpressionFactory;
import org.polypheny.db.util.Util;


/**
 * Implementation of {@link AlgOptTable}.
 */
public class AlgOptTableImpl extends Prepare.AbstractPreparingTable {

    private final transient AlgOptSchema schema;
    private final AlgDataType rowType;
    @Getter
    @Nullable
    private final Table table;

    @Getter
    @Nullable
    private final CatalogTable catalogTable;
    @Nullable
    private final transient Function<Class<?>, Expression> expressionFunction;
    private final ImmutableList<String> names;

    /**
     * Estimate for the row count, or null.
     * <p>
     * If not null, overrides the estimate from the actual table.
     */
    private final Double rowCount;


    private AlgOptTableImpl(
            AlgOptSchema schema,
            AlgDataType rowType,
            List<String> names,
            Table table,
            CatalogTable catalogTable,
            Function<Class<?>, Expression> expressionFunction,
            Double rowCount ) {
        this.schema = schema;
        this.rowType = Objects.requireNonNull( rowType );
        this.names = ImmutableList.copyOf( names );
        this.table = table; // may be null
        this.catalogTable = catalogTable;
        this.expressionFunction = expressionFunction; // may be null
        this.rowCount = rowCount; // may be null
    }


    public static AlgOptTableImpl create( AlgOptSchema schema, AlgDataType rowType, List<String> names, Expression expression ) {
        return new AlgOptTableImpl( schema, rowType, names, null, null, c -> expression, null );
    }


    public static AlgOptTableImpl create( AlgOptSchema schema, AlgDataType rowType, final PolyphenyDbSchema.TableEntry tableEntry, CatalogTable catalogTable, Double count ) {
        final Table table = tableEntry.getTable();
        Double rowCount;
        if ( count == null ) {
            rowCount = table.getStatistic().getRowCount();
        } else {
            rowCount = count;
        }

        return new AlgOptTableImpl( schema, rowType, tableEntry.path(), table, catalogTable, getClassExpressionFunction( tableEntry, table ), rowCount );
    }


    /**
     * Creates a copy of this RelOptTable. The new RelOptTable will have newRowType.
     */
    public AlgOptTableImpl copy( AlgDataType newRowType ) {
        return new AlgOptTableImpl( this.schema, newRowType, this.names, this.table, this.catalogTable, this.expressionFunction, this.rowCount );
    }


    private static Function<Class<?>, Expression> getClassExpressionFunction( PolyphenyDbSchema.TableEntry tableEntry, Table table ) {
        return getClassExpressionFunction( tableEntry.schema.plus(), tableEntry.name, table );
    }


    private static Function<Class<?>, Expression> getClassExpressionFunction( final SchemaPlus schema, final String tableName, final Table table ) {
        if ( table instanceof QueryableTable ) {
            final QueryableTable queryableTable = (QueryableTable) table;
            return clazz -> queryableTable.getExpression( schema, tableName, clazz );
        } else if ( table instanceof ScannableTable
                || table instanceof FilterableTable
                || table instanceof ProjectableFilterableTable ) {
            return clazz -> Schemas.tableExpression( schema, Object[].class, tableName, table.getClass() );
        } else if ( table instanceof StreamableTable ) {
            return getClassExpressionFunction( schema, tableName, ((StreamableTable) table).stream() );
        } else {
            return input -> {
                throw new UnsupportedOperationException();
            };
        }
    }


    public static AlgOptTableImpl create( AlgOptSchema schema, AlgDataType rowType, Table table, CatalogTable catalogTable, ImmutableList<String> names ) {
        assert table instanceof TranslatableTable
                || table instanceof ScannableTable
                || table instanceof ModifiableTable;
        return new AlgOptTableImpl( schema, rowType, names, table, catalogTable, null, null );
    }


    @Override
    public <T> T unwrap( Class<T> clazz ) {
        if ( clazz.isInstance( this ) ) {
            return clazz.cast( this );
        }
        if ( clazz.isInstance( table ) ) {
            return clazz.cast( table );
        }
        if ( table instanceof Wrapper ) {
            final T t = ((Wrapper) table).unwrap( clazz );
            if ( t != null ) {
                return t;
            }
        }
        if ( clazz == PolyphenyDbSchema.class ) {
            return clazz.cast( Schemas.subSchema( ((PolyphenyDbCatalogReader) schema).rootSchema, Util.skipLast( getQualifiedName() ) ) );
        }
        return null;
    }


    @Override
    public Expression getExpression( Class<?> clazz ) {
        if ( expressionFunction == null ) {
            return null;
        }
        return expressionFunction.apply( clazz );
    }


    @Override
    protected AlgOptTable extend( Table extendedTable ) {
        final AlgDataType extendedRowType = extendedTable.getRowType( AlgDataTypeFactory.DEFAULT );
        return new AlgOptTableImpl(
                getRelOptSchema(),
                extendedRowType,
                getQualifiedName(),
                extendedTable,
                null,
                expressionFunction,
                getRowCount() );
    }


    @Override
    public boolean equals( Object obj ) {
        return obj instanceof AlgOptTableImpl
                && this.rowType.equals( ((AlgOptTableImpl) obj).getRowType() )
                && this.table == ((AlgOptTableImpl) obj).table;
    }


    @Override
    public int hashCode() {
        return (this.table == null) ? super.hashCode() : this.table.hashCode();
    }


    @Override
    public double getRowCount() {
        if ( rowCount != null ) {
            return rowCount;
        }
        if ( table != null ) {
            final Double rowCount = table.getStatistic().getRowCount();
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
            final AlgOptTable algOptTable = this.copy( staticRowType );
            return algOptTable.toAlg( context, traitSet );
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
            final AlgOptTable algOptTable =
                    new AlgOptTableImpl( this.schema, b.build(), this.names, this.table, this.catalogTable, this.expressionFunction, this.rowCount ) {
                        @Override
                        public <T> T unwrap( Class<T> clazz ) {
                            if ( clazz.isAssignableFrom( InitializerExpressionFactory.class ) ) {
                                return clazz.cast( NullInitializerExpressionFactory.INSTANCE );
                            }
                            return super.unwrap( clazz );
                        }
                    };
            return algOptTable.toAlg( context, traitSet );
        }

        if ( table instanceof TranslatableTable ) {
            return ((TranslatableTable) table).toAlg( context, this, traitSet );
        }
        final AlgOptCluster cluster = context.getCluster();
        if ( Hook.ENABLE_BINDABLE.get( false ) ) {
            return LogicalScan.create( cluster, this );
        }
        if ( PolyphenyDbPrepareImpl.ENABLE_ENUMERABLE && table instanceof QueryableTable ) {
            return EnumerableScan.create( cluster, this );
        }
        if ( table instanceof ScannableTable
                || table instanceof FilterableTable
                || table instanceof ProjectableFilterableTable ) {
            return LogicalScan.create( cluster, this );
        }
        if ( PolyphenyDbPrepareImpl.ENABLE_ENUMERABLE ) {
            return EnumerableScan.create( cluster, this );
        }
        throw new AssertionError();
    }


    @Override
    public List<AlgCollation> getCollationList() {
        if ( table != null ) {
            return table.getStatistic().getCollations();
        }
        return ImmutableList.of();
    }


    @Override
    public AlgDistribution getDistribution() {
        if ( table != null ) {
            return table.getStatistic().getDistribution();
        }
        return AlgDistributionTraitDef.INSTANCE.getDefault();
    }


    @Override
    public boolean isKey( ImmutableBitSet columns ) {
        if ( table != null ) {
            return table.getStatistic().isKey( columns );
        }
        return false;
    }


    @Override
    public List<AlgReferentialConstraint> getReferentialConstraints() {
        if ( table != null ) {
            return table.getStatistic().getReferentialConstraints();
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
                return table instanceof StreamableTable;
            default:
                return !(table instanceof StreamableTable);
        }
    }


    @Override
    public List<String> getQualifiedName() {
        return names;
    }


    @Override
    public Monotonicity getMonotonicity( String columnName ) {
        for ( AlgCollation collation : table.getStatistic().getCollations() ) {
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
    public static List<ColumnStrategy> columnStrategies( final AlgOptTable table ) {
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
    public static int realOrdinal( final AlgOptTable table, int i ) {
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
    public static AlgDataType realRowType( AlgOptTable table ) {
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


/*
 * Copyright 2019-2020 The Polypheny Project
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
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import lombok.Getter;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.enumerable.EnumerableTableScan;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptSchema;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelDistribution;
import org.polypheny.db.rel.RelDistributionTraitDef;
import org.polypheny.db.rel.RelFieldCollation;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelReferentialConstraint;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rel.type.RelProtoDataType;
import org.polypheny.db.rel.type.RelRecordType;
import org.polypheny.db.runtime.Hook;
import org.polypheny.db.schema.ColumnStrategy;
import org.polypheny.db.schema.FilterableTable;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.Path;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.ProjectableFilterableTable;
import org.polypheny.db.schema.QueryableTable;
import org.polypheny.db.schema.ScannableTable;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.SchemaVersion;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.schema.StreamableTable;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.TranslatableTable;
import org.polypheny.db.schema.Wrapper;
import org.polypheny.db.sql.SqlAccessType;
import org.polypheny.db.sql.validate.SqlModality;
import org.polypheny.db.sql.validate.SqlMonotonicity;
import org.polypheny.db.sql2rel.InitializerExpressionFactory;
import org.polypheny.db.sql2rel.NullInitializerExpressionFactory;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Implementation of {@link org.polypheny.db.plan.RelOptTable}.
 */
public class RelOptTableImpl extends Prepare.AbstractPreparingTable {

    private final RelOptSchema schema;
    private final RelDataType rowType;
    @Getter
    private final Table table;
    private final Function<Class, Expression> expressionFunction;
    private final ImmutableList<String> names;

    /**
     * Estimate for the row count, or null.
     *
     * If not null, overrides the estimate from the actual table.
     */
    private final Double rowCount;


    private RelOptTableImpl( RelOptSchema schema, RelDataType rowType, List<String> names, Table table, Function<Class, Expression> expressionFunction, Double rowCount ) {
        this.schema = schema;
        this.rowType = Objects.requireNonNull( rowType );
        this.names = ImmutableList.copyOf( names );
        this.table = table; // may be null
        this.expressionFunction = expressionFunction; // may be null
        this.rowCount = rowCount; // may be null
    }


    public static RelOptTableImpl create( RelOptSchema schema, RelDataType rowType, List<String> names, Expression expression ) {
        return new RelOptTableImpl( schema, rowType, names, null, c -> expression, null );
    }


    public static RelOptTableImpl create( RelOptSchema schema, RelDataType rowType, Table table, Path path ) {
        final SchemaPlus schemaPlus = MySchemaPlus.create( path );
        return new RelOptTableImpl(
                schema,
                rowType,
                Pair.left( path ),
                table,
                getClassExpressionFunction( schemaPlus, Util.last( path ).left, table ),
                table.getStatistic().getRowCount() );
    }


    public static RelOptTableImpl create( RelOptSchema schema, RelDataType rowType, final PolyphenyDbSchema.TableEntry tableEntry, Double rowCount ) {
        final Table table = tableEntry.getTable();
        return new RelOptTableImpl( schema, rowType, tableEntry.path(), table, getClassExpressionFunction( tableEntry, table ), rowCount );
    }


    /**
     * Creates a copy of this RelOptTable. The new RelOptTable will have newRowType.
     */
    public RelOptTableImpl copy( RelDataType newRowType ) {
        return new RelOptTableImpl( this.schema, newRowType, this.names, this.table, this.expressionFunction, this.rowCount );
    }


    private static Function<Class, Expression> getClassExpressionFunction( PolyphenyDbSchema.TableEntry tableEntry, Table table ) {
        return getClassExpressionFunction( tableEntry.schema.plus(), tableEntry.name, table );
    }


    private static Function<Class, Expression> getClassExpressionFunction( final SchemaPlus schema, final String tableName, final Table table ) {
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


    public static RelOptTableImpl create( RelOptSchema schema, RelDataType rowType, Table table, ImmutableList<String> names ) {
        assert table instanceof TranslatableTable
                || table instanceof ScannableTable
                || table instanceof ModifiableTable;
        return new RelOptTableImpl( schema, rowType, names, table, null, null );
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
    public Expression getExpression( Class clazz ) {
        if ( expressionFunction == null ) {
            return null;
        }
        return expressionFunction.apply( clazz );
    }


    @Override
    protected RelOptTable extend( Table extendedTable ) {
        final RelDataType extendedRowType = extendedTable.getRowType( getRelOptSchema().getTypeFactory() );
        return new RelOptTableImpl( getRelOptSchema(), extendedRowType, getQualifiedName(), extendedTable, expressionFunction, getRowCount() );
    }


    @Override
    public boolean equals( Object obj ) {
        return obj instanceof RelOptTableImpl
                && this.rowType.equals( ((RelOptTableImpl) obj).getRowType() )
                && this.table == ((RelOptTableImpl) obj).table;
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
    public RelOptSchema getRelOptSchema() {
        return schema;
    }


    @Override
    public RelNode toRel( ToRelContext context ) {
        // Make sure rowType's list is immutable. If rowType is DynamicRecordType, creates a new RelOptTable by replacing with
        // immutable RelRecordType using the same field list.
        if ( this.getRowType().isDynamicStruct() ) {
            final RelDataType staticRowType = new RelRecordType( getRowType().getFieldList() );
            final RelOptTable relOptTable = this.copy( staticRowType );
            return relOptTable.toRel( context );
        }

        // If there are any virtual columns, create a copy of this table without those virtual columns.
        final List<ColumnStrategy> strategies = getColumnStrategies();
        if ( strategies.contains( ColumnStrategy.VIRTUAL ) ) {
            final RelDataTypeFactory.Builder b = context.getCluster().getTypeFactory().builder();
            for ( RelDataTypeField field : rowType.getFieldList() ) {
                if ( strategies.get( field.getIndex() ) != ColumnStrategy.VIRTUAL ) {
                    b.add( field.getName(), null, field.getType() );
                }
            }
            final RelOptTable relOptTable =
                    new RelOptTableImpl( this.schema, b.build(), this.names, this.table, this.expressionFunction, this.rowCount ) {
                        @Override
                        public <T> T unwrap( Class<T> clazz ) {
                            if ( clazz.isAssignableFrom( InitializerExpressionFactory.class ) ) {
                                return clazz.cast( NullInitializerExpressionFactory.INSTANCE );
                            }
                            return super.unwrap( clazz );
                        }
                    };
            return relOptTable.toRel( context );
        }

        if ( table instanceof TranslatableTable ) {
            return ((TranslatableTable) table).toRel( context, this );
        }
        final RelOptCluster cluster = context.getCluster();
        if ( Hook.ENABLE_BINDABLE.get( false ) ) {
            return LogicalTableScan.create( cluster, this );
        }
        if ( PolyphenyDbPrepareImpl.ENABLE_ENUMERABLE && table instanceof QueryableTable ) {
            return EnumerableTableScan.create( cluster, this );
        }
        if ( table instanceof ScannableTable
                || table instanceof FilterableTable
                || table instanceof ProjectableFilterableTable ) {
            return LogicalTableScan.create( cluster, this );
        }
        if ( PolyphenyDbPrepareImpl.ENABLE_ENUMERABLE ) {
            return EnumerableTableScan.create( cluster, this );
        }
        throw new AssertionError();
    }


    @Override
    public List<RelCollation> getCollationList() {
        if ( table != null ) {
            return table.getStatistic().getCollations();
        }
        return ImmutableList.of();
    }


    @Override
    public RelDistribution getDistribution() {
        if ( table != null ) {
            return table.getStatistic().getDistribution();
        }
        return RelDistributionTraitDef.INSTANCE.getDefault();
    }


    @Override
    public boolean isKey( ImmutableBitSet columns ) {
        if ( table != null ) {
            return table.getStatistic().isKey( columns );
        }
        return false;
    }


    @Override
    public List<RelReferentialConstraint> getReferentialConstraints() {
        if ( table != null ) {
            return table.getStatistic().getReferentialConstraints();
        }
        return ImmutableList.of();
    }


    @Override
    public RelDataType getRowType() {
        return rowType;
    }


    @Override
    public boolean supportsModality( SqlModality modality ) {
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
    public SqlMonotonicity getMonotonicity( String columnName ) {
        for ( RelCollation collation : table.getStatistic().getCollations() ) {
            final RelFieldCollation fieldCollation = collation.getFieldCollations().get( 0 );
            final int fieldIndex = fieldCollation.getFieldIndex();
            if ( fieldIndex < rowType.getFieldCount() && rowType.getFieldNames().get( fieldIndex ).equals( columnName ) ) {
                return fieldCollation.direction.monotonicity();
            }
        }
        return SqlMonotonicity.NOT_MONOTONIC;
    }


    @Override
    public SqlAccessType getAllowedAccess() {
        return SqlAccessType.ALL;
    }


    /**
     * Helper for {@link #getColumnStrategies()}.
     */
    public static List<ColumnStrategy> columnStrategies( final RelOptTable table ) {
        final int fieldCount = table.getRowType().getFieldCount();
        final InitializerExpressionFactory ief = Util.first( table.unwrap( InitializerExpressionFactory.class ), NullInitializerExpressionFactory.INSTANCE );
        return new AbstractList<ColumnStrategy>() {
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
    public static int realOrdinal( final RelOptTable table, int i ) {
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
     * Returns the row type of a table after any {@link ColumnStrategy#VIRTUAL} columns have been removed. This is the type of the records that are actually stored.
     */
    public static RelDataType realRowType( RelOptTable table ) {
        final RelDataType rowType = table.getRowType();
        final List<ColumnStrategy> strategies = columnStrategies( table );
        if ( !strategies.contains( ColumnStrategy.VIRTUAL ) ) {
            return rowType;
        }
        final RelDataTypeFactory.Builder builder = table.getRelOptSchema().getTypeFactory().builder();
        for ( RelDataTypeField field : rowType.getFieldList() ) {
            if ( strategies.get( field.getIndex() ) != ColumnStrategy.VIRTUAL ) {
                builder.add( field );
            }
        }
        return builder.build();
    }


    /**
     * Implementation of {@link SchemaPlus} that wraps a regular schema and knows its name and parent.
     *
     * It is read-only, and functionality is limited in other ways, it but allows table expressions to be generated.
     */
    private static class MySchemaPlus implements SchemaPlus {

        private final SchemaPlus parent;
        private final String name;
        private final Schema schema;


        MySchemaPlus( SchemaPlus parent, String name, Schema schema ) {
            this.parent = parent;
            this.name = name;
            this.schema = schema;
        }


        public static MySchemaPlus create( Path path ) {
            final Pair<String, Schema> pair = Util.last( path );
            final SchemaPlus parent;
            if ( path.size() == 1 ) {
                parent = null;
            } else {
                parent = create( path.parent() );
            }
            return new MySchemaPlus( parent, pair.left, pair.right );
        }


        @Override
        public PolyphenyDbSchema polyphenyDbSchema() {
            return null;
        }


        @Override
        public SchemaPlus getParentSchema() {
            return parent;
        }


        @Override
        public String getName() {
            return name;
        }


        @Override
        public SchemaPlus getSubSchema( String name ) {
            final Schema subSchema = schema.getSubSchema( name );
            return subSchema == null ? null : new MySchemaPlus( this, name, subSchema );
        }


        @Override
        public SchemaPlus add( String name, Schema schema ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public void add( String name, Table table ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public void add( String name, org.polypheny.db.schema.Function function ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public void add( String name, RelProtoDataType type ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public boolean isMutable() {
            return schema.isMutable();
        }


        @Override
        public <T> T unwrap( Class<T> clazz ) {
            return null;
        }


        @Override
        public void setPath( ImmutableList<ImmutableList<String>> path ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public void setCacheEnabled( boolean cache ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public boolean isCacheEnabled() {
            return false;
        }


        @Override
        public Table getTable( String name ) {
            return schema.getTable( name );
        }


        @Override
        public Set<String> getTableNames() {
            return schema.getTableNames();
        }


        @Override
        public RelProtoDataType getType( String name ) {
            return schema.getType( name );
        }


        @Override
        public Set<String> getTypeNames() {
            return schema.getTypeNames();
        }


        @Override
        public Collection<org.polypheny.db.schema.Function>
        getFunctions( String name ) {
            return schema.getFunctions( name );
        }


        @Override
        public Set<String> getFunctionNames() {
            return schema.getFunctionNames();
        }


        @Override
        public Set<String> getSubSchemaNames() {
            return schema.getSubSchemaNames();
        }


        @Override
        public Expression getExpression( SchemaPlus parentSchema, String name ) {
            return schema.getExpression( parentSchema, name );
        }


        @Override
        public Schema snapshot( SchemaVersion version ) {
            throw new UnsupportedOperationException();
        }
    }
}


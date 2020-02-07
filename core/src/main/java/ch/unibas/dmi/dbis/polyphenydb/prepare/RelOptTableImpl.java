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

package ch.unibas.dmi.dbis.polyphenydb.prepare;


import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableTableScan;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptSchema;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelDistribution;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelDistributionTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelFieldCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelReferentialConstraint;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalTableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelRecordType;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Hook;
import ch.unibas.dmi.dbis.polyphenydb.schema.ColumnStrategy;
import ch.unibas.dmi.dbis.polyphenydb.schema.FilterableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.ModifiableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.Path;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.ProjectableFilterableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.QueryableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.ScannableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaVersion;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schemas;
import ch.unibas.dmi.dbis.polyphenydb.schema.StreamableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.schema.TranslatableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.Wrapper;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlAccessType;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlModality;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlMonotonicity;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.InitializerExpressionFactory;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.NullInitializerExpressionFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ImmutableList;
import java.util.AbstractList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.apache.calcite.linq4j.tree.Expression;


/**
 * Implementation of {@link ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable}.
 */
public class RelOptTableImpl extends Prepare.AbstractPreparingTable {

    private final RelOptSchema schema;
    private final RelDataType rowType;
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
        // Make sure rowType's list is immutable. If rowType is DynamicRecordType, creates a new RelOptTable by replacing with immutable RelRecordType using the same field list.
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
        public void add( String name, ch.unibas.dmi.dbis.polyphenydb.schema.Function function ) {
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
        public Collection<ch.unibas.dmi.dbis.polyphenydb.schema.Function>
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


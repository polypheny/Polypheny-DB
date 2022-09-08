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
 */

package org.polypheny.db.catalog;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgDistribution;
import org.polypheny.db.algebra.AlgDistributions;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgReferentialConstraint;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Modality;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.logical.relational.LogicalScan;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeComparability;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFamily;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypePrecedenceList;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.algebra.type.DynamicRecordTypeImpl;
import org.polypheny.db.algebra.type.StructKind;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.plan.AlgOptSchema;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.prepare.Prepare;
import org.polypheny.db.schema.AbstractPolyphenyDbSchema;
import org.polypheny.db.schema.CustomColumnResolvingTable;
import org.polypheny.db.schema.ExtensibleTable;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Statistic;
import org.polypheny.db.schema.StreamableTable;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.Wrapper;
import org.polypheny.db.schema.impl.AbstractSchema;
import org.polypheny.db.test.JdbcTest;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.AccessType;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.InitializerExpressionFactory;
import org.polypheny.db.util.NameMatcher;
import org.polypheny.db.util.NameMatchers;
import org.polypheny.db.util.NullInitializerExpressionFactory;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.ValidatorUtil;


/**
 * Mock implementation of {#@link SqlValidatorCatalogReader} which returns tables "EMP", "DEPT", "BONUS", "SALGRADE" (same as Oracle's SCOTT schema).
 * Also two streams "ORDERS", "SHIPMENTS"; and a view "EMP_20".
 */
public abstract class MockCatalogReader extends PolyphenyDbCatalogReader {

    static final String DEFAULT_CATALOG = "CATALOG";
    static final String DEFAULT_SCHEMA = "SALES";
    static final List<String> PREFIX = ImmutableList.of( DEFAULT_SCHEMA );


    /**
     * Creates a MockCatalogReader.
     *
     * Caller must then call {@link #init} to populate with data.
     *
     * @param typeFactory Type factory
     */
    public MockCatalogReader( AlgDataTypeFactory typeFactory, boolean caseSensitive ) {
        super(
                AbstractPolyphenyDbSchema.createRootSchema( DEFAULT_CATALOG ),
                NameMatchers.withCaseSensitive( caseSensitive ),
                ImmutableList.of( PREFIX, ImmutableList.of() ),
                typeFactory );
    }


    @Override
    public NameMatcher nameMatcher() {
        return nameMatcher;
    }


    /**
     * Initializes this catalog reader.
     */
    public abstract MockCatalogReader init();


    protected void registerTablesWithRollUp( MockSchema schema, Fixture f ) {
        // Register "EMP_R" table. Contains a rolled up column.
        final MockTable empRolledTable = MockTable.create( this, schema, "EMP_R", false, 14 );
        empRolledTable.addColumn( "EMPNO", f.intType, true );
        empRolledTable.addColumn( "DEPTNO", f.intType );
        empRolledTable.addColumn( "SLACKER", f.booleanType );
        empRolledTable.addColumn( "SLACKINGMIN", f.intType );
        empRolledTable.registerRolledUpColumn( "SLACKINGMIN" );
        registerTable( empRolledTable );

        // Register the "DEPT_R" table. Doesn't contain a rolled up column,
        // but is useful for testing join
        MockTable deptSlackingTable = MockTable.create( this, schema, "DEPT_R", false, 4 );
        deptSlackingTable.addColumn( "DEPTNO", f.intType, true );
        deptSlackingTable.addColumn( "SLACKINGMIN", f.intType );
        registerTable( deptSlackingTable );

        // Register nested schema NEST that contains table with a rolled up column.
        MockSchema nestedSchema = new MockSchema( "NEST" );
        registerNestedSchema( schema, nestedSchema );

        // Register "EMP_R" table which contains a rolled up column in NEST schema.
        ImmutableList<String> tablePath = ImmutableList.of( schema.getCatalogName(), schema.name, nestedSchema.name, "EMP_R" );
        final MockTable nestedEmpRolledTable = MockTable.create( this, tablePath, false, 14 );
        nestedEmpRolledTable.addColumn( "EMPNO", f.intType, true );
        nestedEmpRolledTable.addColumn( "DEPTNO", f.intType );
        nestedEmpRolledTable.addColumn( "SLACKER", f.booleanType );
        nestedEmpRolledTable.addColumn( "SLACKINGMIN", f.intType );
        nestedEmpRolledTable.registerRolledUpColumn( "SLACKINGMIN" );
        registerTable( nestedEmpRolledTable );
    }


    protected void registerType( final List<String> names, final AlgProtoDataType algProtoDataType ) {
        assert names.get( 0 ).equals( DEFAULT_CATALOG );
        final List<String> schemaPath = Util.skipLast( names );
        final PolyphenyDbSchema schema = ValidatorUtil.getSchema( rootSchema, schemaPath, NameMatchers.withCaseSensitive( true ) );
        schema.add( Util.last( names ), algProtoDataType );
    }


    protected void registerTable( final MockTable table ) {
        table.onRegister( typeFactory );
        final WrapperTable wrapperTable = new WrapperTable( table );
        if ( table.stream ) {
            registerTable(
                    table.names,
                    new StreamableWrapperTable( table ) {
                        @Override
                        public Table stream() {
                            return wrapperTable;
                        }
                    } );
        } else {
            registerTable( table.names, wrapperTable );
        }
    }


    private void registerTable( final List<String> names, final Table table ) {
        assert names.get( 0 ).equals( DEFAULT_CATALOG );
        final List<String> schemaPath = Util.skipLast( names );
        final String tableName = Util.last( names );
        final PolyphenyDbSchema schema = ValidatorUtil.getSchema( rootSchema, schemaPath, NameMatchers.withCaseSensitive( true ) );
        schema.add( tableName, table );
    }


    protected void registerSchema( MockSchema schema ) {
        rootSchema.add( schema.name, new AbstractSchema(), NamespaceType.RELATIONAL );
    }


    private void registerNestedSchema( MockSchema parentSchema, MockSchema schema ) {
        rootSchema.getSubSchema( parentSchema.getName(), true ).add( schema.name, new AbstractSchema(), NamespaceType.RELATIONAL );
    }


    private static List<AlgCollation> deduceMonotonicity( Prepare.PreparingTable table ) {
        final List<AlgCollation> collationList = new ArrayList<>();

        // Deduce which fields the table is sorted on.
        int i = -1;
        for ( AlgDataTypeField field : table.getRowType().getFieldList() ) {
            ++i;
            final Monotonicity monotonicity = table.getMonotonicity( field.getName() );
            if ( monotonicity != Monotonicity.NOT_MONOTONIC ) {
                final AlgFieldCollation.Direction direction =
                        monotonicity.isDecreasing()
                                ? AlgFieldCollation.Direction.DESCENDING
                                : AlgFieldCollation.Direction.ASCENDING;
                collationList.add( AlgCollations.of( new AlgFieldCollation( i, direction ) ) );
            }
        }
        return collationList;
    }


    /**
     * Column resolver
     */
    public interface ColumnResolver {

        List<Pair<AlgDataTypeField, List<String>>> resolveColumn( AlgDataType rowType, AlgDataTypeFactory typeFactory, List<String> names );

    }


    /**
     * Mock schema.
     */
    public static class MockSchema {

        private final List<String> tableNames = new ArrayList<>();
        private String name;


        public MockSchema( String name ) {
            this.name = name;
        }


        public void addTable( String name ) {
            tableNames.add( name );
        }


        public String getCatalogName() {
            return DEFAULT_CATALOG;
        }


        public String getName() {
            return name;
        }

    }


    /**
     * Mock implementation of
     * {@link Prepare.PreparingTable}.
     */
    public static class MockTable extends Prepare.AbstractPreparingTable {

        protected final MockCatalogReader catalogReader;
        protected final boolean stream;
        protected final double rowCount;
        protected final List<Map.Entry<String, AlgDataType>> columnList = new ArrayList<>();
        protected final List<Integer> keyList = new ArrayList<>();
        protected final List<AlgReferentialConstraint> referentialConstraints = new ArrayList<>();
        protected AlgDataType rowType;
        protected List<AlgCollation> collationList;
        protected final List<String> names;
        protected final Set<String> monotonicColumnSet = new HashSet<>();
        protected StructKind kind = StructKind.FULLY_QUALIFIED;
        protected final ColumnResolver resolver;
        protected final InitializerExpressionFactory initializerFactory;
        protected final Set<String> rolledUpColumns = new HashSet<>();


        public MockTable(
                MockCatalogReader catalogReader, String catalogName, String schemaName, String name, boolean stream, double rowCount,
                ColumnResolver resolver, InitializerExpressionFactory initializerFactory ) {
            this( catalogReader, ImmutableList.of( catalogName, schemaName, name ), stream, rowCount, resolver, initializerFactory );
        }


        public void registerRolledUpColumn( String columnName ) {
            rolledUpColumns.add( columnName );
        }


        private MockTable( MockCatalogReader catalogReader, List<String> names, boolean stream, double rowCount, ColumnResolver resolver, InitializerExpressionFactory initializerFactory ) {
            this.catalogReader = catalogReader;
            this.stream = stream;
            this.rowCount = rowCount;
            this.names = names;
            this.resolver = resolver;
            this.initializerFactory = initializerFactory;
        }


        /**
         * Copy constructor.
         */
        protected MockTable(
                MockCatalogReader catalogReader, boolean stream, double rowCount, List<Map.Entry<String, AlgDataType>> columnList, List<Integer> keyList, AlgDataType rowType, List<AlgCollation> collationList,
                List<String> names, Set<String> monotonicColumnSet, StructKind kind, ColumnResolver resolver, InitializerExpressionFactory initializerFactory ) {
            this.catalogReader = catalogReader;
            this.stream = stream;
            this.rowCount = rowCount;
            this.rowType = rowType;
            this.collationList = collationList;
            this.names = names;
            this.kind = kind;
            this.resolver = resolver;
            this.initializerFactory = initializerFactory;
            for ( String name : monotonicColumnSet ) {
                addMonotonic( name );
            }
        }


        /**
         * Implementation of AbstractModifiableTable.
         */
        private class ModifiableTable extends JdbcTest.AbstractModifiableTable implements ExtensibleTable, Wrapper {

            protected ModifiableTable( String tableName ) {
                super( tableName );
            }


            @Override
            public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
                return typeFactory.createStructType( MockTable.this.getRowType().getFieldList() );
            }


            @Override
            public Collection getModifiableCollection() {
                return null;
            }


            @Override
            public <E> Queryable<E> asQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
                return null;
            }


            @Override
            public Type getElementType() {
                return null;
            }


            @Override
            public Expression getExpression( SchemaPlus schema, String tableName, Class clazz ) {
                return null;
            }


            @Override
            public <C> C unwrap( Class<C> aClass ) {
                if ( aClass.isInstance( initializerFactory ) ) {
                    return aClass.cast( initializerFactory );
                } else if ( aClass.isInstance( MockTable.this ) ) {
                    return aClass.cast( MockTable.this );
                }
                return super.unwrap( aClass );
            }


            @Override
            public Table extend( final List<AlgDataTypeField> fields ) {
                return new ModifiableTable( Util.last( names ) ) {
                    @Override
                    public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
                        ImmutableList<AlgDataTypeField> allFields = ImmutableList.copyOf( Iterables.concat( ModifiableTable.this.getRowType( typeFactory ).getFieldList(), fields ) );
                        return typeFactory.createStructType( allFields );
                    }
                };
            }


            @Override
            public int getExtendedColumnOffset() {
                return rowType.getFieldCount();
            }

        }


        @Override
        protected AlgOptTable extend( final Table extendedTable ) {
            return new MockTable( catalogReader, names, stream, rowCount, resolver, initializerFactory ) {
                @Override
                public AlgDataType getRowType() {
                    return extendedTable.getRowType( catalogReader.typeFactory );
                }
            };
        }


        public static MockTable create( MockCatalogReader catalogReader, MockSchema schema, String name, boolean stream, double rowCount ) {
            return create( catalogReader, schema, name, stream, rowCount, null );
        }


        public static MockTable create( MockCatalogReader catalogReader, List<String> names, boolean stream, double rowCount ) {
            return new MockTable( catalogReader, names, stream, rowCount, null, NullInitializerExpressionFactory.INSTANCE );
        }


        public static MockTable create(
                MockCatalogReader catalogReader,
                MockSchema schema, String name, boolean stream, double rowCount,
                ColumnResolver resolver ) {
            return create( catalogReader, schema, name, stream, rowCount, resolver,
                    NullInitializerExpressionFactory.INSTANCE );
        }


        public static MockTable create( MockCatalogReader catalogReader, MockSchema schema, String name, boolean stream, double rowCount, ColumnResolver resolver, InitializerExpressionFactory initializerExpressionFactory ) {
            MockTable table = new MockTable( catalogReader, schema.getCatalogName(), schema.name, name, stream, rowCount, resolver, initializerExpressionFactory );
            schema.addTable( name );
            return table;
        }


        @Override
        public <T> T unwrap( Class<T> clazz ) {
            if ( clazz.isInstance( this ) ) {
                return clazz.cast( this );
            }
            if ( clazz.isInstance( initializerFactory ) ) {
                return clazz.cast( initializerFactory );
            }
            if ( clazz.isAssignableFrom( Table.class ) ) {
                final Table table = resolver == null
                        ? new ModifiableTable( Util.last( names ) )
                        : new ModifiableTableWithCustomColumnResolving( Util.last( names ) );
                return clazz.cast( table );
            }
            return null;
        }


        @Override
        public double getRowCount() {
            return rowCount;
        }


        @Override
        public AlgOptSchema getRelOptSchema() {
            return catalogReader;
        }


        @Override
        public AlgNode toAlg( ToAlgContext context ) {
            return LogicalScan.create( context.getCluster(), this );
        }


        @Override
        public List<AlgCollation> getCollationList() {
            return collationList;
        }


        @Override
        public AlgDistribution getDistribution() {
            return AlgDistributions.BROADCAST_DISTRIBUTED;
        }


        @Override
        public boolean isKey( ImmutableBitSet columns ) {
            return !keyList.isEmpty() && columns.contains( ImmutableBitSet.of( keyList ) );
        }


        @Override
        public List<AlgReferentialConstraint> getReferentialConstraints() {
            return referentialConstraints;
        }


        @Override
        public AlgDataType getRowType() {
            return rowType;
        }


        @Override
        public boolean supportsModality( Modality modality ) {
            return modality == (stream ? Modality.STREAM : Modality.RELATION);
        }


        public void onRegister( AlgDataTypeFactory typeFactory ) {
            rowType = typeFactory.createStructType( kind, Pair.right( columnList ), Pair.left( columnList ) );
            collationList = deduceMonotonicity( this );
        }


        @Override
        public List<String> getQualifiedName() {
            return names;
        }


        @Override
        public Monotonicity getMonotonicity( String columnName ) {
            return monotonicColumnSet.contains( columnName )
                    ? Monotonicity.INCREASING
                    : Monotonicity.NOT_MONOTONIC;
        }


        @Override
        public AccessType getAllowedAccess() {
            return AccessType.ALL;
        }


        @Override
        public Expression getExpression( Class clazz ) {
            throw new UnsupportedOperationException();
        }


        public void addColumn( String name, AlgDataType type ) {
            addColumn( name, type, false );
        }


        public void addColumn( String name, AlgDataType type, boolean isKey ) {
            if ( isKey ) {
                keyList.add( columnList.size() );
            }
            columnList.add( Pair.of( name, type ) );
        }


        public void addMonotonic( String name ) {
            monotonicColumnSet.add( name );
            assert Pair.left( columnList ).contains( name );
        }


        public void setKind( StructKind kind ) {
            this.kind = kind;
        }


        public StructKind getKind() {
            return kind;
        }


        /**
         * Subclass of {@link ModifiableTable} that also implements {@link CustomColumnResolvingTable}.
         */
        private class ModifiableTableWithCustomColumnResolving extends ModifiableTable implements CustomColumnResolvingTable, Wrapper {

            ModifiableTableWithCustomColumnResolving( String tableName ) {
                super( tableName );
            }


            @Override
            public List<Pair<AlgDataTypeField, List<String>>> resolveColumn( AlgDataType rowType, AlgDataTypeFactory typeFactory, List<String> names ) {
                return resolver.resolveColumn( rowType, typeFactory, names );
            }

        }

    }


    /**
     * Mock implementation of {@link Prepare.PreparingTable} with dynamic record type.
     */
    public static class MockDynamicTable extends MockTable {

        public MockDynamicTable( MockCatalogReader catalogReader, String catalogName, String schemaName, String name, boolean stream, double rowCount ) {
            super( catalogReader, catalogName, schemaName, name, stream, rowCount, null, NullInitializerExpressionFactory.INSTANCE );
        }


        @Override
        public void onRegister( AlgDataTypeFactory typeFactory ) {
            rowType = new DynamicRecordTypeImpl( typeFactory );
        }


        /**
         * Recreates an immutable rowType, if the table has Dynamic Record Type, when converts table to Rel.
         */
        @Override
        public AlgNode toAlg( ToAlgContext context ) {
            if ( rowType.isDynamicStruct() ) {
                rowType = new AlgRecordType( rowType.getFieldList() );
            }
            return super.toAlg( context );
        }

    }


    /**
     * Struct type based on another struct type.
     */
    private static class DelegateStructType implements AlgDataType {

        private AlgDataType delegate;
        private StructKind structKind;


        DelegateStructType( AlgDataType delegate, StructKind structKind ) {
            assert delegate.isStruct();
            this.delegate = delegate;
            this.structKind = structKind;
        }


        @Override
        public boolean isStruct() {
            return delegate.isStruct();
        }


        @Override
        public boolean isDynamicStruct() {
            return delegate.isDynamicStruct();
        }


        @Override
        public List<AlgDataTypeField> getFieldList() {
            return delegate.getFieldList();
        }


        @Override
        public List<String> getFieldNames() {
            return delegate.getFieldNames();
        }


        @Override
        public int getFieldCount() {
            return delegate.getFieldCount();
        }


        @Override
        public StructKind getStructKind() {
            return structKind;
        }


        @Override
        public AlgDataTypeField getField( String fieldName, boolean caseSensitive, boolean elideRecord ) {
            return delegate.getField( fieldName, caseSensitive, elideRecord );
        }


        @Override
        public boolean isNullable() {
            return delegate.isNullable();
        }


        @Override
        public AlgDataType getComponentType() {
            return delegate.getComponentType();
        }


        @Override
        public AlgDataType getKeyType() {
            return delegate.getKeyType();
        }


        @Override
        public AlgDataType getValueType() {
            return delegate.getValueType();
        }


        @Override
        public Charset getCharset() {
            return delegate.getCharset();
        }


        @Override
        public Collation getCollation() {
            return delegate.getCollation();
        }


        @Override
        public IntervalQualifier getIntervalQualifier() {
            return delegate.getIntervalQualifier();
        }


        @Override
        public int getPrecision() {
            return delegate.getPrecision();
        }


        @Override
        public int getRawPrecision() {
            return delegate.getRawPrecision();
        }


        @Override
        public int getScale() {
            return delegate.getScale();
        }


        @Override
        public PolyType getPolyType() {
            return delegate.getPolyType();
        }


        @Override
        public Identifier getSqlIdentifier() {
            return delegate.getSqlIdentifier();
        }


        @Override
        public String getFullTypeString() {
            return delegate.getFullTypeString();
        }


        @Override
        public AlgDataTypeFamily getFamily() {
            return delegate.getFamily();
        }


        @Override
        public AlgDataTypePrecedenceList getPrecedenceList() {
            return delegate.getPrecedenceList();
        }


        @Override
        public AlgDataTypeComparability getComparability() {
            return delegate.getComparability();
        }

    }


    /**
     * Wrapper around a {@link MockTable}, giving it a {@link Table} interface. You can get the {@code MockTable} by calling {@link #unwrap(Class)}.
     */
    private static class WrapperTable implements Table, Wrapper {

        private final MockTable table;


        WrapperTable( MockTable table ) {
            this.table = table;
        }


        @Override
        public <C> C unwrap( Class<C> aClass ) {
            return aClass.isInstance( this )
                    ? aClass.cast( this )
                    : aClass.isInstance( table )
                            ? aClass.cast( table )
                            : null;
        }


        @Override
        public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
            return table.getRowType();
        }


        @Override
        public Statistic getStatistic() {
            return new Statistic() {
                @Override
                public Double getRowCount() {
                    return table.rowCount;
                }


                @Override
                public boolean isKey( ImmutableBitSet columns ) {
                    return table.isKey( columns );
                }


                @Override
                public List<AlgReferentialConstraint> getReferentialConstraints() {
                    return table.getReferentialConstraints();
                }


                @Override
                public List<AlgCollation> getCollations() {
                    return table.collationList;
                }


                @Override
                public AlgDistribution getDistribution() {
                    return table.getDistribution();
                }
            };
        }


        @Override
        public Long getTableId() {
            throw new RuntimeException( "Method getTableId is not implemented." );
        }


        @Override
        public boolean isRolledUp( String column ) {
            return table.rolledUpColumns.contains( column );
        }


        @Override
        public boolean rolledUpColumnValidInsideAgg( String column, Call call, Node parent ) {
            // For testing
            return call.getKind() != Kind.MAX && (parent.getKind() == Kind.SELECT || parent.getKind() == Kind.FILTER);
        }


        @Override
        public Schema.TableType getJdbcTableType() {
            return table.stream ? Schema.TableType.STREAM : Schema.TableType.TABLE;
        }

    }


    /**
     * Wrapper around a {@link MockTable}, giving it a {@link StreamableTable} interface.
     */
    private static class StreamableWrapperTable extends WrapperTable implements StreamableTable {

        StreamableWrapperTable( MockTable table ) {
            super( table );
        }


        @Override
        public Table stream() {
            return this;
        }

    }

}

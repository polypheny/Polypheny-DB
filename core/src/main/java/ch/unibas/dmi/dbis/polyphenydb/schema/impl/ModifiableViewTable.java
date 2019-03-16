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

package ch.unibas.dmi.dbis.polyphenydb.schema.impl;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.schema.ColumnStrategy;
import ch.unibas.dmi.dbis.polyphenydb.schema.ExtensibleTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.ModifiableView;
import ch.unibas.dmi.dbis.polyphenydb.schema.Path;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.schema.Wrapper;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.InitializerContext;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.InitializerExpressionFactory;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.NullInitializerExpressionFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableIntList;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Extension to {@link ViewTable} that is modifiable.
 */
public class ModifiableViewTable extends ViewTable implements ModifiableView, Wrapper {

    private final Table table;
    private final Path tablePath;
    private final RexNode constraint;
    private final ImmutableIntList columnMapping;
    private final InitializerExpressionFactory initializerExpressionFactory;


    /**
     * Creates a ModifiableViewTable.
     */
    public ModifiableViewTable( Type elementType, RelProtoDataType rowType, String viewSql, List<String> schemaPath, List<String> viewPath, Table table, Path tablePath, RexNode constraint, ImmutableIntList columnMapping ) {
        super( elementType, rowType, viewSql, schemaPath, viewPath );
        this.table = table;
        this.tablePath = tablePath;
        this.constraint = constraint;
        this.columnMapping = columnMapping;
        this.initializerExpressionFactory = new ModifiableViewTableInitializerExpressionFactory();
    }


    public RexNode getConstraint( RexBuilder rexBuilder, RelDataType tableRowType ) {
        return rexBuilder.copy( constraint );
    }


    public ImmutableIntList getColumnMapping() {
        return columnMapping;
    }


    public Table getTable() {
        return table;
    }


    public Path getTablePath() {
        return tablePath;
    }


    @Override
    public <C> C unwrap( Class<C> aClass ) {
        if ( aClass.isInstance( initializerExpressionFactory ) ) {
            return aClass.cast( initializerExpressionFactory );
        } else if ( aClass.isInstance( table ) ) {
            return aClass.cast( table );
        }
        return super.unwrap( aClass );
    }


    /**
     * Extends the underlying table and returns a new view with updated row-type and column-mapping.
     *
     * The type factory is used to perform some scratch calculations, viz the type mapping, but the "real" row-type will be assigned later, when the table has been bound to the statement's type factory.
     * The is important, because adding types to type factories that do not belong to a statement could potentially leak memory.
     *
     * @param extendedColumns Extended fields
     * @param typeFactory Type factory
     */
    public final ModifiableViewTable extend( List<RelDataTypeField> extendedColumns, RelDataTypeFactory typeFactory ) {
        final ExtensibleTable underlying = unwrap( ExtensibleTable.class );
        assert underlying != null;

        final RelDataTypeFactory.Builder builder = typeFactory.builder();
        final RelDataType rowType = getRowType( typeFactory );
        for ( RelDataTypeField column : rowType.getFieldList() ) {
            builder.add( column );
        }
        for ( RelDataTypeField column : extendedColumns ) {
            builder.add( column );
        }

        // The characteristics of the new view.
        final RelDataType newRowType = builder.build();
        final ImmutableIntList newColumnMapping = getNewColumnMapping( underlying, getColumnMapping(), extendedColumns, typeFactory );

        // Extend the underlying table with only the fields that duplicate column names in neither the view nor the base table.
        final List<RelDataTypeField> underlyingColumns = underlying.getRowType( typeFactory ).getFieldList();
        final List<RelDataTypeField> columnsOfExtendedBaseTable = RelOptUtil.deduplicateColumns( underlyingColumns, extendedColumns );
        final List<RelDataTypeField> extendColumnsOfBaseTable = columnsOfExtendedBaseTable.subList( underlyingColumns.size(), columnsOfExtendedBaseTable.size() );
        final Table extendedTable = underlying.extend( extendColumnsOfBaseTable );

        return extend( extendedTable, RelDataTypeImpl.proto( newRowType ), newColumnMapping );
    }


    /**
     * Creates a mapping from the view index to the index in the underlying table.
     */
    private static ImmutableIntList getNewColumnMapping( Table underlying, ImmutableIntList oldColumnMapping, List<RelDataTypeField> extendedColumns, RelDataTypeFactory typeFactory ) {
        final List<RelDataTypeField> baseColumns = underlying.getRowType( typeFactory ).getFieldList();
        final Map<String, Integer> nameToIndex = SqlValidatorUtil.mapNameToIndex( baseColumns );

        final ImmutableList.Builder<Integer> newMapping = ImmutableList.builder();
        newMapping.addAll( oldColumnMapping );
        int newMappedIndex = baseColumns.size();
        for ( RelDataTypeField extendedColumn : extendedColumns ) {
            if ( nameToIndex.containsKey( extendedColumn.getName() ) ) {
                // The extended column duplicates a column in the underlying table.
                // Map to the index in the underlying table.
                newMapping.add( nameToIndex.get( extendedColumn.getName() ) );
            } else {
                // The extended column is not in the underlying table.
                newMapping.add( newMappedIndex++ );
            }
        }
        return ImmutableIntList.copyOf( newMapping.build() );
    }


    protected ModifiableViewTable extend( Table extendedTable, RelProtoDataType protoRowType, ImmutableIntList newColumnMapping ) {
        return new ModifiableViewTable( getElementType(), protoRowType, getViewSql(), getSchemaPath(), getViewPath(), extendedTable, getTablePath(), constraint, newColumnMapping );
    }


    /**
     * Initializes columns based on the view constraint.
     */
    private class ModifiableViewTableInitializerExpressionFactory extends NullInitializerExpressionFactory {

        private final ImmutableMap<Integer, RexNode> projectMap;


        private ModifiableViewTableInitializerExpressionFactory() {
            super();
            final Map<Integer, RexNode> projectMap = new HashMap<>();
            final List<RexNode> filters = new ArrayList<>();
            RelOptUtil.inferViewPredicates( projectMap, filters, constraint );
            assert filters.isEmpty();
            this.projectMap = ImmutableMap.copyOf( projectMap );
        }


        @Override
        public ColumnStrategy generationStrategy( RelOptTable table, int iColumn ) {
            final ModifiableViewTable viewTable = table.unwrap( ModifiableViewTable.class );
            assert iColumn < viewTable.columnMapping.size();

            // Use the view constraint to generate the default value if the column is constrained.
            final int mappedOrdinal = viewTable.columnMapping.get( iColumn );
            final RexNode viewConstraint = projectMap.get( mappedOrdinal );
            if ( viewConstraint != null ) {
                return ColumnStrategy.DEFAULT;
            }

            // Otherwise use the default value of the underlying table.
            final Table schemaTable = viewTable.getTable();
            if ( schemaTable instanceof Wrapper ) {
                final InitializerExpressionFactory initializerExpressionFactory = ((Wrapper) schemaTable).unwrap( InitializerExpressionFactory.class );
                if ( initializerExpressionFactory != null ) {
                    return initializerExpressionFactory.generationStrategy( table, iColumn );
                }
            }
            return super.generationStrategy( table, iColumn );
        }


        @Override
        public RexNode newColumnDefaultValue( RelOptTable table, int iColumn, InitializerContext context ) {
            final ModifiableViewTable viewTable = table.unwrap( ModifiableViewTable.class );
            assert iColumn < viewTable.columnMapping.size();
            final RexBuilder rexBuilder = context.getRexBuilder();
            final RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
            final RelDataType viewType = viewTable.getRowType( typeFactory );
            final RelDataType iType = viewType.getFieldList().get( iColumn ).getType();

            // Use the view constraint to generate the default value if the column is constrained.
            final int mappedOrdinal = viewTable.columnMapping.get( iColumn );
            final RexNode viewConstraint = projectMap.get( mappedOrdinal );
            if ( viewConstraint != null ) {
                return rexBuilder.ensureType( iType, viewConstraint, true );
            }

            // Otherwise use the default value of the underlying table.
            final Table schemaTable = viewTable.getTable();
            if ( schemaTable instanceof Wrapper ) {
                final InitializerExpressionFactory initializerExpressionFactory = ((Wrapper) schemaTable).unwrap( InitializerExpressionFactory.class );
                if ( initializerExpressionFactory != null ) {
                    final RexNode tableConstraint = initializerExpressionFactory.newColumnDefaultValue( table, iColumn, context );
                    return rexBuilder.ensureType( iType, tableConstraint, true );
                }
            }

            // Otherwise Sql type of NULL.
            return super.newColumnDefaultValue( table, iColumn, context );
        }


        @Override
        public RexNode newAttributeInitializer( RelDataType type, SqlFunction constructor, int iAttribute, List<RexNode> constructorArgs, InitializerContext context ) {
            throw new UnsupportedOperationException( "Not implemented - unknown requirements" );
        }
    }
}


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

package ch.unibas.dmi.dbis.polyphenydb.sql.validate;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory.Builder;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.schema.ExtensibleTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.ModifiableViewTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNodeList;
import ch.unibas.dmi.dbis.polyphenydb.util.Static;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * Namespace based on a table from the catalog.
 */
class TableNamespace extends AbstractNamespace {

    private final SqlValidatorTable table;
    public final ImmutableList<RelDataTypeField> extendedFields;


    /**
     * Creates a TableNamespace.
     */
    private TableNamespace( SqlValidatorImpl validator, SqlValidatorTable table, List<RelDataTypeField> fields ) {
        super( validator, null );
        this.table = Objects.requireNonNull( table );
        this.extendedFields = ImmutableList.copyOf( fields );
    }


    TableNamespace( SqlValidatorImpl validator, SqlValidatorTable table ) {
        this( validator, table, ImmutableList.of() );
    }


    protected RelDataType validateImpl( RelDataType targetRowType ) {
        if ( extendedFields.isEmpty() ) {
            return table.getRowType();
        }
        final Builder builder = validator.getTypeFactory().builder();
        builder.addAll( table.getRowType().getFieldList() );
        builder.addAll( extendedFields );
        return builder.build();
    }


    public SqlNode getNode() {
        // This is the only kind of namespace not based on a node in the parse tree.
        return null;
    }


    @Override
    public SqlValidatorTable getTable() {
        return table;
    }


    @Override
    public SqlMonotonicity getMonotonicity( String columnName ) {
        final SqlValidatorTable table = getTable();
        return table.getMonotonicity( columnName );
    }


    /**
     * Creates a TableNamespace based on the same table as this one, but with extended fields.
     *
     * Extended fields are "hidden" or undeclared fields that may nevertheless be present if you ask for them. Phoenix uses them, for instance, to access
     * rarely used fields in the underlying HBase table.
     */
    public TableNamespace extend( SqlNodeList extendList ) {
        final List<SqlNode> identifierList = Util.quotientList( extendList.getList(), 2, 0 );
        SqlValidatorUtil.checkIdentifierListForDuplicates( identifierList, validator.getValidationErrorFunction() );
        final ImmutableList.Builder<RelDataTypeField> builder = ImmutableList.builder();
        builder.addAll( this.extendedFields );
        builder.addAll( SqlValidatorUtil.getExtendedColumns( validator.getTypeFactory(), getTable(), extendList ) );
        final List<RelDataTypeField> extendedFields = builder.build();
        final Table schemaTable = table.unwrap( Table.class );
        if ( schemaTable != null && table instanceof RelOptTable && (schemaTable instanceof ExtensibleTable || schemaTable instanceof ModifiableViewTable) ) {
            checkExtendedColumnTypes( extendList );
            final RelOptTable relOptTable = ((RelOptTable) table).extend( extendedFields );
            final SqlValidatorTable validatorTable = relOptTable.unwrap( SqlValidatorTable.class );
            return new TableNamespace( validator, validatorTable, ImmutableList.of() );
        }
        return new TableNamespace( validator, table, extendedFields );
    }


    /**
     * Gets the data-type of all columns in a table (for a view table: including columns of the underlying table)
     */
    private RelDataType getBaseRowType() {
        final Table schemaTable = table.unwrap( Table.class );
        if ( schemaTable instanceof ModifiableViewTable ) {
            final Table underlying = ((ModifiableViewTable) schemaTable).unwrap( Table.class );
            assert underlying != null;
            return underlying.getRowType( validator.typeFactory );
        }
        return schemaTable.getRowType( validator.typeFactory );
    }


    /**
     * Ensures that extended columns that have the same name as a base column also have the same data-type.
     */
    private void checkExtendedColumnTypes( SqlNodeList extendList ) {
        final List<RelDataTypeField> extendedFields = SqlValidatorUtil.getExtendedColumns( validator.getTypeFactory(), table, extendList );
        final List<RelDataTypeField> baseFields = getBaseRowType().getFieldList();
        final Map<String, Integer> nameToIndex = SqlValidatorUtil.mapNameToIndex( baseFields );

        for ( final RelDataTypeField extendedField : extendedFields ) {
            final String extFieldName = extendedField.getName();
            if ( nameToIndex.containsKey( extFieldName ) ) {
                final Integer baseIndex = nameToIndex.get( extFieldName );
                final RelDataType baseType = baseFields.get( baseIndex ).getType();
                final RelDataType extType = extendedField.getType();

                if ( !extType.equals( baseType ) ) {
                    // Get the extended column node that failed validation.
                    final SqlNode extColNode =
                            Iterables.find(
                                    extendList.getList(),
                                    sqlNode -> sqlNode instanceof SqlIdentifier && Util.last( ((SqlIdentifier) sqlNode).names ).equals( extendedField.getName() ) );

                    throw validator.getValidationErrorFunction().apply(
                            extColNode,
                            Static.RESOURCE.typeNotAssignable(
                                    baseFields.get( baseIndex ).getName(),
                                    baseType.getFullTypeString(),
                                    extendedField.getName(),
                                    extType.getFullTypeString() ) );
                }
            }
        }
    }
}


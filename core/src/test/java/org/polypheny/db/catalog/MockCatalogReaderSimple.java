/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.catalog;


import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.type.ObjectPolyType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.InitializerExpressionFactory;
import org.polypheny.db.util.Litmus;


/**
 * Simple catalog reader for testing.
 */
public class MockCatalogReaderSimple extends MockCatalogReader {

    @Getter
    private final Fixture fixture;


    /**
     * Creates a MockCatalogReader.
     *
     * Caller must then call {@link #init} to populate with data.
     *
     * @param typeFactory Type factory
     * @param caseSensitive case sensitivity
     */
    public MockCatalogReaderSimple( AlgDataTypeFactory typeFactory, boolean caseSensitive ) {
        super( typeFactory, caseSensitive );
        fixture = new Fixture( typeFactory );
    }


    @Override
    public AlgDataType getNamedType( Identifier typeName ) {
        if ( typeName.equalsDeep( fixture.addressType.getSqlIdentifier(), Litmus.IGNORE ) ) {
            return fixture.addressType;
        } else {
            return super.getNamedType( typeName );
        }
    }


    @Override
    public MockCatalogReader init() {
        ObjectPolyType addressType = fixture.addressType;

        // Register "SALES" schema.
        MockSchema salesSchema = new MockSchema( "SALES" );
        registerSchema( salesSchema );

        // Register "EMP" table with customer InitializerExpressionFactory to check whether newDefaultValue method called or not.
        final InitializerExpressionFactory countingInitializerExpressionFactory = new CountingFactory( ImmutableList.of( "DEPTNO" ) );

        registerType( ImmutableList.of( salesSchema.getCatalogName(), salesSchema.getName(), "customBigInt" ), typeFactory -> typeFactory.createPolyType( PolyType.BIGINT ) );

        // Register "EMP" table.
        final MockTable empTable = MockTable.create( this, salesSchema, "EMP", false, 14, null, countingInitializerExpressionFactory );
        empTable.addColumn( "EMPNO", fixture.intType, true );
        empTable.addColumn( "ENAME", fixture.varchar20Type );
        empTable.addColumn( "JOB", fixture.varchar10Type );
        empTable.addColumn( "MGR", fixture.intTypeNull );
        empTable.addColumn( "HIREDATE", fixture.timestampType );
        empTable.addColumn( "SAL", fixture.intType );
        empTable.addColumn( "COMM", fixture.intType );
        empTable.addColumn( "DEPTNO", fixture.intType );
        empTable.addColumn( "SLACKER", fixture.booleanType );
        registerTable( empTable );

        // Register "EMPNULLABLES" table with nullable columns.
        final MockTable empNullablesTable = MockTable.create( this, salesSchema, "EMPNULLABLES", false, 14 );
        empNullablesTable.addColumn( "EMPNO", fixture.intType, true );
        empNullablesTable.addColumn( "ENAME", fixture.varchar20Type );
        empNullablesTable.addColumn( "JOB", fixture.varchar10TypeNull );
        empNullablesTable.addColumn( "MGR", fixture.intTypeNull );
        empNullablesTable.addColumn( "HIREDATE", fixture.timestampTypeNull );
        empNullablesTable.addColumn( "SAL", fixture.intTypeNull );
        empNullablesTable.addColumn( "COMM", fixture.intTypeNull );
        empNullablesTable.addColumn( "DEPTNO", fixture.intTypeNull );
        empNullablesTable.addColumn( "SLACKER", fixture.booleanTypeNull );
        registerTable( empNullablesTable );

        // Register "EMPDEFAULTS" table with default values for some columns.
        final MockTable empDefaultsTable = MockTable.create( this, salesSchema, "EMPDEFAULTS", false, 14, null, new EmpInitializerExpressionFactory() );
        empDefaultsTable.addColumn( "EMPNO", fixture.intType, true );
        empDefaultsTable.addColumn( "ENAME", fixture.varchar20Type );
        empDefaultsTable.addColumn( "JOB", fixture.varchar10TypeNull );
        empDefaultsTable.addColumn( "MGR", fixture.intTypeNull );
        empDefaultsTable.addColumn( "HIREDATE", fixture.timestampTypeNull );
        empDefaultsTable.addColumn( "SAL", fixture.intTypeNull );
        empDefaultsTable.addColumn( "COMM", fixture.intTypeNull );
        empDefaultsTable.addColumn( "DEPTNO", fixture.intTypeNull );
        empDefaultsTable.addColumn( "SLACKER", fixture.booleanTypeNull );
        registerTable( empDefaultsTable );

        // Register "EMP_B" table. As "EMP", birth with a "BIRTHDATE" column.
        final MockTable empBTable = MockTable.create( this, salesSchema, "EMP_B", false, 14 );
        empBTable.addColumn( "EMPNO", fixture.intType, true );
        empBTable.addColumn( "ENAME", fixture.varchar20Type );
        empBTable.addColumn( "JOB", fixture.varchar10Type );
        empBTable.addColumn( "MGR", fixture.intTypeNull );
        empBTable.addColumn( "HIREDATE", fixture.timestampType );
        empBTable.addColumn( "SAL", fixture.intType );
        empBTable.addColumn( "COMM", fixture.intType );
        empBTable.addColumn( "DEPTNO", fixture.intType );
        empBTable.addColumn( "SLACKER", fixture.booleanType );
        empBTable.addColumn( "BIRTHDATE", fixture.dateType );
        registerTable( empBTable );

        // Register "DEPT" table.
        MockTable deptTable = MockTable.create( this, salesSchema, "DEPT", false, 4 );
        deptTable.addColumn( "DEPTNO", fixture.intType, true );
        deptTable.addColumn( "NAME", fixture.varchar10Type );
        registerTable( deptTable );

        // Register "DEPT_NESTED" table.
        MockTable deptNestedTable = MockTable.create( this, salesSchema, "DEPT_NESTED", false, 4 );
        deptNestedTable.addColumn( "DEPTNO", fixture.intType, true );
        deptNestedTable.addColumn( "NAME", fixture.varchar10Type );
        deptNestedTable.addColumn( "SKILL", fixture.skillRecordType );
        deptNestedTable.addColumn( "EMPLOYEES", fixture.empListType );
        registerTable( deptNestedTable );

        // Register "BONUS" table.
        MockTable bonusTable = MockTable.create( this, salesSchema, "BONUS", false, 0 );
        bonusTable.addColumn( "ENAME", fixture.varchar20Type );
        bonusTable.addColumn( "JOB", fixture.varchar10Type );
        bonusTable.addColumn( "SAL", fixture.intType );
        bonusTable.addColumn( "COMM", fixture.intType );
        registerTable( bonusTable );

        // Register "SALGRADE" table.
        MockTable salgradeTable = MockTable.create( this, salesSchema, "SALGRADE", false, 5 );
        salgradeTable.addColumn( "GRADE", fixture.intType, true );
        salgradeTable.addColumn( "LOSAL", fixture.intType );
        salgradeTable.addColumn( "HISAL", fixture.intType );
        registerTable( salgradeTable );

        // Register "EMP_ADDRESS" table
        MockTable contactAddressTable = MockTable.create( this, salesSchema, "EMP_ADDRESS", false, 26 );
        contactAddressTable.addColumn( "EMPNO", fixture.intType, true );
        contactAddressTable.addColumn( "HOME_ADDRESS", addressType );
        contactAddressTable.addColumn( "MAILING_ADDRESS", addressType );
        registerTable( contactAddressTable );

        // Register "CUSTOMER" schema.
        MockSchema customerSchema = new MockSchema( "CUSTOMER" );
        registerSchema( customerSchema );

        // Register "CONTACT" table.
        MockTable contactTable = MockTable.create( this, customerSchema, "CONTACT", false, 1000 );
        contactTable.addColumn( "CONTACTNO", fixture.intType );
        contactTable.addColumn( "FNAME", fixture.varchar10Type );
        contactTable.addColumn( "LNAME", fixture.varchar10Type );
        contactTable.addColumn( "EMAIL", fixture.varchar20Type );
        contactTable.addColumn( "COORD", fixture.rectilinearCoordType );
        registerTable( contactTable );

        // Register "CONTACT_PEEK" table. The
        MockTable contactPeekTable = MockTable.create( this, customerSchema, "CONTACT_PEEK", false, 1000 );
        contactPeekTable.addColumn( "CONTACTNO", fixture.intType );
        contactPeekTable.addColumn( "FNAME", fixture.varchar10Type );
        contactPeekTable.addColumn( "LNAME", fixture.varchar10Type );
        contactPeekTable.addColumn( "EMAIL", fixture.varchar20Type );
        contactPeekTable.addColumn( "COORD", fixture.rectilinearPeekCoordType );
        contactPeekTable.addColumn( "COORD_NE", fixture.rectilinearPeekNoExpandCoordType );
        registerTable( contactPeekTable );

        // Register "ACCOUNT" table.
        MockTable accountTable = MockTable.create( this, customerSchema, "ACCOUNT", false, 457 );
        accountTable.addColumn( "ACCTNO", fixture.intType );
        accountTable.addColumn( "TYPE", fixture.varchar20Type );
        accountTable.addColumn( "BALANCE", fixture.intType );
        registerTable( accountTable );

        // Register "ORDERS" stream.
        MockTable ordersStream = MockTable.create( this, salesSchema, "ORDERS", true, Double.POSITIVE_INFINITY );
        ordersStream.addColumn( "ROWTIME", fixture.timestampType );
        ordersStream.addMonotonic( "ROWTIME" );
        ordersStream.addColumn( "PRODUCTID", fixture.intType );
        ordersStream.addColumn( "ORDERID", fixture.intType );
        registerTable( ordersStream );

        // Register "SHIPMENTS" stream. "ROWTIME" is not column 0, just to mix things up.
        MockTable shipmentsStream = MockTable.create( this, salesSchema, "SHIPMENTS", true, Double.POSITIVE_INFINITY );
        shipmentsStream.addColumn( "ORDERID", fixture.intType );
        shipmentsStream.addColumn( "ROWTIME", fixture.timestampType );
        shipmentsStream.addMonotonic( "ROWTIME" );
        registerTable( shipmentsStream );

        // Register "PRODUCTS" table.
        MockTable productsTable = MockTable.create( this, salesSchema, "PRODUCTS", false, 200D );
        productsTable.addColumn( "PRODUCTID", fixture.intType );
        productsTable.addColumn( "NAME", fixture.varchar20Type );
        productsTable.addColumn( "SUPPLIERID", fixture.intType );
        registerTable( productsTable );

        // Register "SUPPLIERS" table.
        MockTable suppliersTable = MockTable.create( this, salesSchema, "SUPPLIERS", false, 10D );
        suppliersTable.addColumn( "SUPPLIERID", fixture.intType );
        suppliersTable.addColumn( "NAME", fixture.varchar20Type );
        suppliersTable.addColumn( "CITY", fixture.intType );
        registerTable( suppliersTable );

        MockSchema structTypeSchema = new MockSchema( "STRUCT" );
        registerSchema( structTypeSchema );
        final List<CompoundNameColumn> columns = Arrays.asList(
                new CompoundNameColumn( "", "K0", fixture.varchar20Type ),
                new CompoundNameColumn( "", "C1", fixture.varchar20Type ),
                new CompoundNameColumn( "F1", "A0", fixture.intType ),
                new CompoundNameColumn( "F2", "A0", fixture.booleanType ),
                new CompoundNameColumn( "F0", "C0", fixture.intType ),
                new CompoundNameColumn( "F1", "C0", fixture.intTypeNull ),
                new CompoundNameColumn( "F0", "C1", fixture.intType ),
                new CompoundNameColumn( "F1", "C2", fixture.intType ),
                new CompoundNameColumn( "F2", "C3", fixture.intType ) );
        final CompoundNameColumnResolver structTypeTableResolver = new CompoundNameColumnResolver( columns, "F0" );
        final MockTable structTypeTable = MockTable.create( this, structTypeSchema, "T", false, 100, structTypeTableResolver );
        for ( CompoundNameColumn column : columns ) {
            structTypeTable.addColumn( column.getName(), column.type );
        }
        registerTable( structTypeTable );

        final List<CompoundNameColumn> columnsNullable = Arrays.asList(
                new CompoundNameColumn( "", "K0", fixture.varchar20TypeNull ),
                new CompoundNameColumn( "", "C1", fixture.varchar20TypeNull ),
                new CompoundNameColumn( "F1", "A0", fixture.intTypeNull ),
                new CompoundNameColumn( "F2", "A0", fixture.booleanTypeNull ),
                new CompoundNameColumn( "F0", "C0", fixture.intTypeNull ),
                new CompoundNameColumn( "F1", "C0", fixture.intTypeNull ),
                new CompoundNameColumn( "F0", "C1", fixture.intTypeNull ),
                new CompoundNameColumn( "F1", "C2", fixture.intType ),
                new CompoundNameColumn( "F2", "C3", fixture.intTypeNull ) );
        final MockTable structNullableTypeTable = MockTable.create( this, structTypeSchema, "T_NULLABLES", false, 100, structTypeTableResolver );
        for ( CompoundNameColumn column : columnsNullable ) {
            structNullableTypeTable.addColumn( column.getName(), column.type );
        }
        registerTable( structNullableTypeTable );

        registerTablesWithRollUp( salesSchema, fixture );
        return this;

    }

}

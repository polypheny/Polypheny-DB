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
 */

package org.polypheny.db.sql;

import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.polypheny.db.PolyphenyDb;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.logistic.ConstraintType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.DdlManager.ColumnTypeInformation;
import org.polypheny.db.ddl.DdlManager.ConstraintInformation;
import org.polypheny.db.ddl.DdlManager.FieldInformation;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.RunMode;

public class SqlLanguageDependent {


    public static TestHelper testHelper;


    @BeforeAll
    public static void startUp() {
        setupSqlAndSchema();
    }


    @AfterAll
    public static void tearDown() {
        removeTestSchema();
        removeHrSchema();
    }


    @SneakyThrows
    private static void removeHrSchema() {

        TransactionManager transactionManager = testHelper.getTransactionManager();

        Transaction transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Sql Test" );

        DdlManager manager = DdlManager.getInstance();

        Catalog.snapshot().rel().getTable( Catalog.defaultNamespaceId, "hr" ).ifPresent( table -> {
            manager.dropTable( table, transaction.createStatement() );
        } );
        transaction.commit();
    }


    @SneakyThrows
    private static void removeTestSchema() {
        TransactionManager transactionManager = testHelper.getTransactionManager();

        Transaction transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Sql Test" );

        DdlManager manager = DdlManager.getInstance();

        Catalog.snapshot().rel().getTable( Catalog.defaultNamespaceId, "dept" ).ifPresent( table -> {
            manager.dropTable( table, transaction.createStatement() );
        } );

        Catalog.snapshot().rel().getTable( Catalog.defaultNamespaceId, "emp" ).ifPresent( table -> {
            manager.dropTable( table, transaction.createStatement() );
        } );
        transaction.commit();
    }


    @SneakyThrows
    public static void setupSqlAndSchema() {
        PolyphenyDb.mode = RunMode.TEST;
        testHelper = TestHelper.getInstance();
        createTestSchema( testHelper );
        createHrSchema( testHelper );
    }


    private static void createHrSchema( TestHelper testHelper ) throws TransactionException {

        TransactionManager transactionManager = testHelper.getTransactionManager();

        Transaction transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Sql Test" );

        DdlManager manager = DdlManager.getInstance();

        List<FieldInformation> columns = List.of(
                new FieldInformation( "deptno", new ColumnTypeInformation( PolyType.INTEGER, null, null, null, null, null, false ), null, null, 0 ),
                new FieldInformation( "name", new ColumnTypeInformation( PolyType.VARCHAR, null, 20, null, null, null, false ), null, null, 1 ),
                new FieldInformation( "loc", new ColumnTypeInformation( PolyType.VARCHAR, null, 50, null, null, null, true ), null, null, 2 )
        );

        List<ConstraintInformation> constraints = List.of(
                new ConstraintInformation( "PRIMARY KEY", ConstraintType.PRIMARY, List.of( "deptno" ), null, null )
        );

        manager.createTable( Catalog.defaultNamespaceId, "hr", columns, constraints, true, null, null, transaction.createStatement() );

        transaction.commit();

    }


    private static void createTestSchema( TestHelper testHelper ) {
        // "CREATE TABLE department( deptno INTEGER NOT NULL, name VARCHAR(20) NOT NULL, loc VARCHAR(50) NULL, PRIMARY KEY (deptno))"

        TransactionManager transactionManager = testHelper.getTransactionManager();

        Transaction transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Sql Test" );

        DdlManager manager = DdlManager.getInstance();

        List<FieldInformation> columns = List.of(
                new FieldInformation( "deptno", new ColumnTypeInformation( PolyType.INTEGER, null, null, null, null, null, false ), null, null, 0 ),
                new FieldInformation( "name", new ColumnTypeInformation( PolyType.VARCHAR, null, 20, null, null, null, false ), null, null, 1 ),
                new FieldInformation( "loc", new ColumnTypeInformation( PolyType.VARCHAR, null, 50, null, null, null, true ), null, null, 2 )
        );

        List<ConstraintInformation> constraints = List.of(
                new ConstraintInformation( "PRIMARY KEY", ConstraintType.PRIMARY, List.of( "deptno" ), null, null )
        );

        manager.createTable( Catalog.defaultNamespaceId, "dept", columns, constraints, true, null, null, transaction.createStatement() );

        // "CREATE TABLE employee( empid BIGINT NOT NULL, ename VARCHAR(20), job VARCHAR(10), mgr INTEGER, hiredate DATE, salary DECIMAL(7,2), commission DECIMAL(7,2), deptno INTEGER NOT NULL, PRIMARY KEY (empid)) "
        columns = List.of(
                new FieldInformation( "empid", new ColumnTypeInformation( PolyType.BIGINT, null, null, null, null, null, false ), null, null, 0 ),
                new FieldInformation( "ename", new ColumnTypeInformation( PolyType.VARCHAR, null, 20, null, null, null, true ), null, null, 1 ),
                new FieldInformation( "job", new ColumnTypeInformation( PolyType.VARCHAR, null, 10, null, null, null, true ), null, null, 2 ),
                new FieldInformation( "mgr", new ColumnTypeInformation( PolyType.INTEGER, null, null, null, null, null, true ), null, null, 3 ),
                new FieldInformation( "hiredate", new ColumnTypeInformation( PolyType.DATE, null, null, null, null, null, true ), null, null, 4 ),
                new FieldInformation( "salary", new ColumnTypeInformation( PolyType.DECIMAL, null, null, 7, 2, null, true ), null, null, 5 ),
                new FieldInformation( "deptno", new ColumnTypeInformation( PolyType.INTEGER, null, null, null, null, null, true ), null, null, 6 )
        );

        constraints = List.of(
                new ConstraintInformation( "PRIMARY KEY", ConstraintType.PRIMARY, List.of( "empid" ), null, null )
        );

        manager.createTable( Catalog.defaultNamespaceId, "emp", columns, constraints, true, null, null, transaction.createStatement() );

        // "CREATE TABLE contact.customer(fname VARCHAR(50) NOT NULL, PRIMARY KEY (fname))"
        long id = manager.createNamespace( "customer", DataModel.RELATIONAL, true, false, transaction.createStatement() );

        columns = List.of(
                new FieldInformation( "fname", new ColumnTypeInformation( PolyType.VARCHAR, null, 50, null, null, null, false ), null, null, 0 )
        );

        constraints = List.of(
                new ConstraintInformation( "PRIMARY KEY", ConstraintType.PRIMARY, List.of( "fname" ), null, null )
        );

        manager.createTable( id, "contact", columns, constraints, true, null, null, transaction.createStatement() );

        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            throw new RuntimeException( e );
        }
    }

}

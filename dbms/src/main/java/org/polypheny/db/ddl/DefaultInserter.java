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
 */

package org.polypheny.db.ddl;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.linq4j.function.Deterministic;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.ConstraintType;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.ForeignKeyOption;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.ddl.DdlManager.ColumnTypeInformation;
import org.polypheny.db.ddl.DdlManager.ConstraintInformation;
import org.polypheny.db.ddl.DdlManager.FieldInformation;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.iface.QueryInterfaceManager.QueryInterfaceType;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;

@Deterministic
public class DefaultInserter {

    private final Catalog catalog = Catalog.getInstance();
    private final TransactionManager manager;
    private final DdlManager ddlManager;
    private Transaction transaction;


    public DefaultInserter( DdlManager ddlManager, TransactionManager manager ) {
        this.manager = manager;
        this.ddlManager = ddlManager;
        insertDefaultData();
        if ( Catalog.snapshot().getQueryInterface( "avatica" ) == null ) {
            QueryInterfaceType avatica = QueryInterfaceManager.getREGISTER().get( "AvaticaInterface" );
            catalog.addQueryInterface( "avatica", avatica.clazz.getName(), avatica.defaultSettings );
        }
    }


    /**
     * Fills the catalog database with default data, skips if data is already inserted
     */
    private void insertDefaultData() {

        //////////////
        // init users
        long systemId = catalog.addUser( "system", "" );

        catalog.addUser( "pa", "" );

        Catalog.defaultUserId = systemId;

        //////////////
        // init schema

        long namespaceId = catalog.addNamespace( "public", NamespaceType.getDefault(), false );

        //////////////
        // init adapters
        if ( catalog.getAdapters().size() != 0 ) {
            catalog.commit();
            return;
        }

        catalog.updateSnapshot();
        this.transaction = manager.startTransaction( catalog.getSnapshot().getUser( Catalog.defaultUserId ), catalog.getSnapshot().getNamespace( namespaceId ), false, "Defaults" );

        // Deploy default store
        ddlManager.addAdapter( "hsqldb", Catalog.defaultStore.getAdapterName(), AdapterType.STORE, Catalog.defaultStore.getDefaultSettings() );
        //AdapterManager.getInstance().addAdapter( Catalog.defaultStore.getAdapterName(), "hsqldb", AdapterType.STORE, Catalog.defaultStore.getDefaultSettings() );

        // Deploy default CSV view
        //Adapter adapter = AdapterManager.getInstance().addAdapter( Catalog.defaultSource.getAdapterName(), "hr", AdapterType.SOURCE, Catalog.defaultSource.getDefaultSettings() );
        ddlManager.addAdapter( "hr", Catalog.defaultSource.getAdapterName(), AdapterType.SOURCE, Catalog.defaultSource.getDefaultSettings() );
        //adapter.createNewSchema( Catalog.snapshot(), "public", namespaceId );
        // init schema

        //catalog.updateSnapshot();

        //CatalogAdapter csv = catalog.getSnapshot().getAdapter( "hr" );
        //addDefaultCsvColumns( csv, namespaceId );

        catalog.commit();

    }


    /**
     * Initiates default columns for csv files
     */
    private void addDefaultCsvColumns( CatalogAdapter csv, long namespaceId ) {
        LogicalTable depts = getDepts( csv, namespaceId );

        LogicalTable emps = getEmps( csv, namespaceId );

        LogicalTable emp = getEmp( csv, namespaceId );

        LogicalTable work = getWork( csv, namespaceId );

        catalog.updateSnapshot();

        // set foreign keys
        catalog.getLogicalRel( namespaceId ).addForeignKey(
                emps.id,
                ImmutableList.of( catalog.getSnapshot().rel().getColumn( emps.id, "deptno" ).id ),
                depts.id,
                ImmutableList.of( catalog.getSnapshot().rel().getColumn( depts.id, "deptno" ).id ),
                "fk_emps_depts",
                ForeignKeyOption.NONE,
                ForeignKeyOption.NONE );
        catalog.getLogicalRel( namespaceId ).addForeignKey(
                work.id,
                ImmutableList.of( catalog.getSnapshot().rel().getColumn( work.id, "employeeno" ).id ),
                emp.id,
                ImmutableList.of( catalog.getSnapshot().rel().getColumn( emp.id, "employeeno" ).id ),
                "fk_work_emp",
                ForeignKeyOption.NONE,
                ForeignKeyOption.NONE );
    }


    private LogicalTable getWork( CatalogAdapter csv, long namespaceId ) {
        catalog.getLogicalRel( namespaceId ).addTable( "work", EntityType.SOURCE, false );
        LogicalTable work = Catalog.snapshot().rel().getTable( namespaceId, "work" );
        catalog.getLogicalRel( namespaceId ).addPrimaryKey( work.id, Collections.singletonList( catalog.getSnapshot().rel().getColumn( work.id, "employeeno" ).id ) );
        addDefaultCsvColumn( csv, work, "employeeno", PolyType.INTEGER, null, 1, null );
        addDefaultCsvColumn( csv, work, "educationfield", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 2, 20 );
        addDefaultCsvColumn( csv, work, "jobinvolvement", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 3, 20 );
        addDefaultCsvColumn( csv, work, "joblevel", PolyType.INTEGER, null, 4, null );
        addDefaultCsvColumn( csv, work, "jobrole", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 5, 30 );
        addDefaultCsvColumn( csv, work, "businesstravel", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 6, 20 );
        addDefaultCsvColumn( csv, work, "department", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 7, 25 );
        addDefaultCsvColumn( csv, work, "attrition", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 8, 20 );
        addDefaultCsvColumn( csv, work, "dailyrate", PolyType.INTEGER, null, 9, null );
        return work;
    }


    private LogicalTable getEmp( CatalogAdapter csv, long namespaceId ) {
        catalog.getLogicalRel( namespaceId ).addTable( "emp", EntityType.SOURCE, false );
        LogicalTable emp = Catalog.snapshot().rel().getTable( namespaceId, "emp" );
        catalog.getLogicalRel( namespaceId ).addPrimaryKey( emp.id, Collections.singletonList( catalog.getSnapshot().rel().getColumn( emp.id, "employeeno" ).id ) );
        addDefaultCsvColumn( csv, emp, "employeeno", PolyType.INTEGER, null, 1, null );
        addDefaultCsvColumn( csv, emp, "age", PolyType.INTEGER, null, 2, null );
        addDefaultCsvColumn( csv, emp, "gender", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 3, 20 );
        addDefaultCsvColumn( csv, emp, "maritalstatus", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 4, 20 );
        addDefaultCsvColumn( csv, emp, "worklifebalance", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 5, 20 );
        addDefaultCsvColumn( csv, emp, "education", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 6, 20 );
        addDefaultCsvColumn( csv, emp, "monthlyincome", PolyType.INTEGER, null, 7, null );
        addDefaultCsvColumn( csv, emp, "relationshipjoy", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 8, 20 );
        addDefaultCsvColumn( csv, emp, "workingyears", PolyType.INTEGER, null, 9, null );
        addDefaultCsvColumn( csv, emp, "yearsatcompany", PolyType.INTEGER, null, 10, null );
        return emp;
    }


    private LogicalTable getEmps( CatalogAdapter csv, long namespaceId ) {
        catalog.getLogicalRel( namespaceId ).addTable( "emps", EntityType.SOURCE, false );
        LogicalTable emps = Catalog.snapshot().rel().getTable( namespaceId, "emps" );
        catalog.getLogicalRel( namespaceId ).addPrimaryKey( emps.id, Collections.singletonList( catalog.getSnapshot().rel().getColumn( emps.id, "empid" ).id ) );
        addDefaultCsvColumn( csv, emps, "empid", PolyType.INTEGER, null, 1, null );
        addDefaultCsvColumn( csv, emps, "deptno", PolyType.INTEGER, null, 2, null );
        addDefaultCsvColumn( csv, emps, "name", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 3, 20 );
        addDefaultCsvColumn( csv, emps, "salary", PolyType.INTEGER, null, 4, null );
        addDefaultCsvColumn( csv, emps, "commission", PolyType.INTEGER, null, 5, null );
        return emps;
    }


    private LogicalTable getDepts( CatalogAdapter csv, long namespaceId ) {
        List<FieldInformation> fields = List.of(
                new FieldInformation(
                        "deptno",
                        new ColumnTypeInformation( PolyType.INTEGER, null, null, null, null, null, false ),
                        null,
                        null,
                        1 ),
                new FieldInformation(
                        "name",
                        new ColumnTypeInformation( PolyType.VARCHAR, null, 20, null, null, null, false ),
                        Collation.CASE_INSENSITIVE,
                        null,
                        2 )
        );
        List<ConstraintInformation> constraints = List.of(
                new ConstraintInformation( "primary", ConstraintType.PRIMARY, List.of( "deptno" ) )
        );

        //DdlManager.getInstance().createTable( namespaceId, "depts", fields, constraints, true, List.of( AdapterManager.getInstance().getSource( csv.id ) ), PlacementType.AUTOMATIC, transaction.createStatement() );
        catalog.getLogicalRel( namespaceId ).addTable( "depts", EntityType.SOURCE, false );

        LogicalTable depts = Catalog.snapshot().rel().getTable( namespaceId, "depts" );
        catalog.getLogicalRel( namespaceId ).addPrimaryKey( depts.id, Collections.singletonList( catalog.getSnapshot().rel().getColumn( depts.id, "deptno" ).id ) );
        addDefaultCsvColumn( csv, depts, "deptno", PolyType.INTEGER, null, 1, null );
        addDefaultCsvColumn( csv, depts, "name", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 2, 20 );
        return depts;
    }


    private void addDefaultCsvColumn( CatalogAdapter csv, LogicalTable table, String name, PolyType type, Collation collation, int position, Integer length ) {
        if ( catalog.getSnapshot().rel().checkIfExistsColumn( table.id, name ) ) {
            return;
        }
        LogicalColumn column = catalog.getLogicalRel( table.namespaceId ).addColumn( name, table.id, position, type, null, length, null, null, null, false, collation );
        String filename = table.name + ".csv";
        if ( table.name.equals( "emp" ) || table.name.equals( "work" ) ) {
            filename += ".gz";
        }

        catalog.updateSnapshot();
        AllocationEntity alloc;
        if ( !catalog.getSnapshot().alloc().adapterHasPlacement( csv.id, table.id ) ) {
            alloc = catalog.getAllocRel( table.namespaceId ).createAllocationTable( csv.id, table.id );
        } else {
            alloc = catalog.getSnapshot().alloc().getAllocation( csv.id, table.id );
        }

        catalog.getAllocRel( table.namespaceId ).addColumn( alloc.id, column.id, PlacementType.AUTOMATIC, position );
        //getAllocRel( table.namespaceId ).addColumn( alloc.id, colId, PlacementType.AUTOMATIC, filename, table.name, name, position );
        //getAllocRel( table.namespaceId ).updateColumnPlacementPhysicalPosition( allocId, colId, position );

        catalog.updateSnapshot();

        // long partitionId = table.partitionProperty.partitionIds.get( 0 );
        // getAllocRel( table.namespaceId ).addPartitionPlacement( table.namespaceId, csv.id, table.id, partitionId, PlacementType.AUTOMATIC, DataPlacementRole.UPTODATE );

    }


    private void addDefaultColumn( CatalogAdapter adapter, LogicalTable table, String name, PolyType type, Collation collation, int position, Integer length ) {
        /*if ( !getSnapshot().rel().checkIfExistsColumn( table.id, name ) ) {
            LogicalColumn column = getLogicalRel( table.namespaceId ).addColumn( name, table.id, position, type, null, length, null, null, null, false, collation );
            AllocationEntity entity = getSnapshot().alloc().getAllocation( adapter.id, table.id );
            getAllocRel( table.namespaceId ).addColumn( entity.id, column.id, PlacementType.AUTOMATIC, position );
            //getAllocRel( table.namespaceId ).addColumn( entity.id, colId, PlacementType.AUTOMATIC, "col" + colId, table.name, name, position );
            getAllocRel( table.namespaceId ).updateColumnPlacementPhysicalPosition( adapter.id, column.id, position );
        }*/
    }

}

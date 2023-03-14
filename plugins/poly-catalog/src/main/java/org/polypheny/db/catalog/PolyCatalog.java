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

package org.polypheny.db.catalog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.beans.PropertyChangeSupport;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.allocation.PolyAllocDocCatalog;
import org.polypheny.db.catalog.allocation.PolyAllocGraphCatalog;
import org.polypheny.db.catalog.allocation.PolyAllocRelCatalog;
import org.polypheny.db.catalog.catalogs.AllocationCatalog;
import org.polypheny.db.catalog.catalogs.AllocationDocumentCatalog;
import org.polypheny.db.catalog.catalogs.AllocationGraphCatalog;
import org.polypheny.db.catalog.catalogs.AllocationRelationalCatalog;
import org.polypheny.db.catalog.catalogs.LogicalCatalog;
import org.polypheny.db.catalog.catalogs.LogicalDocumentCatalog;
import org.polypheny.db.catalog.catalogs.LogicalGraphCatalog;
import org.polypheny.db.catalog.catalogs.LogicalRelationalCatalog;
import org.polypheny.db.catalog.catalogs.PhysicalCatalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogQueryInterface;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.logical.DocumentCatalog;
import org.polypheny.db.catalog.logical.GraphCatalog;
import org.polypheny.db.catalog.logical.RelationalCatalog;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.ForeignKeyOption;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.catalog.physical.PolyPhysicalCatalog;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.catalog.snapshot.impl.SnapshotBuilder;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;


/**
 * Central catalog, which distributes the operations to the corresponding model catalogs.
 */
@Slf4j
public class PolyCatalog extends Catalog implements Serializable {

    @Getter
    public final BinarySerializer<PolyCatalog> serializer = Serializable.builder.get().build( PolyCatalog.class );

    @Serialize
    public final Map<Long, LogicalCatalog> logicalCatalogs;

    @Serialize
    public final Map<Long, AllocationCatalog> allocationCatalogs;

    @Serialize
    public final Map<Long, PhysicalCatalog> physicalCatalogs;

    @Serialize
    @Getter
    public final Map<Long, CatalogUser> users;

    @Serialize
    @Getter
    public final Map<Long, CatalogAdapter> adapters;

    @Serialize
    @Getter
    public final Map<Long, CatalogQueryInterface> interfaces;

    private final IdBuilder idBuilder = IdBuilder.getInstance();

    protected final PropertyChangeSupport listeners = new PropertyChangeSupport( this );

    @Getter
    private Snapshot snapshot;


    public PolyCatalog() {
        this(
                new ConcurrentHashMap<>(),
                new ConcurrentHashMap<>(),
                new ConcurrentHashMap<>(),
                new ConcurrentHashMap<>(),
                new ConcurrentHashMap<>(),
                new ConcurrentHashMap<>() );

    }


    @Override
    public void init() {
        try {
            insertDefaultData();
        } catch ( UnknownAdapterException e ) {
            throw new RuntimeException( e );
        }
    }


    public PolyCatalog(
            @Deserialize("users") Map<Long, CatalogUser> users,
            @Deserialize("logicalCatalogs") Map<Long, LogicalCatalog> logicalCatalogs,
            @Deserialize("allocationCatalogs") Map<Long, AllocationCatalog> allocationCatalogs,
            @Deserialize("physicalCatalogs") Map<Long, PhysicalCatalog> physicalCatalogs,
            @Deserialize("adapters") Map<Long, CatalogAdapter> adapters,
            @Deserialize("interfaces") Map<Long, CatalogQueryInterface> interfaces ) {

        this.users = users;
        this.logicalCatalogs = logicalCatalogs;
        this.allocationCatalogs = allocationCatalogs;
        this.physicalCatalogs = physicalCatalogs;
        this.adapters = adapters;
        this.interfaces = interfaces;
        updateSnapshot();
    }


    /**
     * Fills the catalog database with default data, skips if data is already inserted
     */
    private void insertDefaultData() throws UnknownAdapterException {

        //////////////
        // init users
        long systemId = addUser( "system", "" );

        addUser( "pa", "" );

        Catalog.defaultUserId = systemId;

        //////////////
        // init schema

        long namespaceId = addNamespace( "public", NamespaceType.getDefault(), false );

        //////////////
        // init adapters
        if ( adapters.size() == 0 ) {
            // Deploy default store
            AdapterManager.getInstance().addAdapter( defaultStore.getAdapterName(), "hsqldb", AdapterType.STORE, defaultStore.getDefaultSettings() );

            // Deploy default CSV view
            Adapter adapter = AdapterManager.getInstance().addAdapter( defaultSource.getAdapterName(), "hr", AdapterType.SOURCE, defaultSource.getDefaultSettings() );

            adapter.createNewSchema( getSnapshot(), "public", namespaceId );
            // init schema

            getLogicalRel( namespaceId ).addTable( "depts", EntityType.SOURCE, false );
            getLogicalRel( namespaceId ).addTable( "emps", EntityType.SOURCE, false );
            getLogicalRel( namespaceId ).addTable( "emp", EntityType.SOURCE, false );
            getLogicalRel( namespaceId ).addTable( "work", EntityType.SOURCE, false );

            updateSnapshot();

            try {
                CatalogAdapter csv = getSnapshot().getAdapter( "hr" );
                addDefaultCsvColumns( csv, namespaceId );
            } catch ( UnknownTableException | GenericCatalogException | UnknownColumnException e ) {
                throw new RuntimeException( e );
            }


        }

        commit();

    }


    /**
     * Initiates default columns for csv files
     */
    private void addDefaultCsvColumns( CatalogAdapter csv, long namespaceId ) throws UnknownTableException, UnknownColumnException, GenericCatalogException {
        LogicalTable depts = getSnapshot().getRelSnapshot( namespaceId ).getTable( "depts" );
        addDefaultCsvColumn( csv, depts, "deptno", PolyType.INTEGER, null, 1, null );
        addDefaultCsvColumn( csv, depts, "name", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 2, 20 );

        LogicalTable emps = getSnapshot().getRelSnapshot( namespaceId ).getTable( "emps" );
        addDefaultCsvColumn( csv, emps, "empid", PolyType.INTEGER, null, 1, null );
        addDefaultCsvColumn( csv, emps, "deptno", PolyType.INTEGER, null, 2, null );
        addDefaultCsvColumn( csv, emps, "name", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 3, 20 );
        addDefaultCsvColumn( csv, emps, "salary", PolyType.INTEGER, null, 4, null );
        addDefaultCsvColumn( csv, emps, "commission", PolyType.INTEGER, null, 5, null );

        LogicalTable emp = getSnapshot().getRelSnapshot( namespaceId ).getTable( "emp" );
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

        LogicalTable work = getSnapshot().getRelSnapshot( namespaceId ).getTable( "work" );
        addDefaultCsvColumn( csv, work, "employeeno", PolyType.INTEGER, null, 1, null );
        addDefaultCsvColumn( csv, work, "educationfield", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 2, 20 );
        addDefaultCsvColumn( csv, work, "jobinvolvement", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 3, 20 );
        addDefaultCsvColumn( csv, work, "joblevel", PolyType.INTEGER, null, 4, null );
        addDefaultCsvColumn( csv, work, "jobrole", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 5, 30 );
        addDefaultCsvColumn( csv, work, "businesstravel", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 6, 20 );
        addDefaultCsvColumn( csv, work, "department", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 7, 25 );
        addDefaultCsvColumn( csv, work, "attrition", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 8, 20 );
        addDefaultCsvColumn( csv, work, "dailyrate", PolyType.INTEGER, null, 9, null );

        updateSnapshot();

        // set all needed primary keys
        getLogicalRel( namespaceId ).addPrimaryKey( depts.id, Collections.singletonList( getSnapshot().getRelSnapshot( namespaceId ).getColumn( depts.id, "deptno" ).id ) );
        getLogicalRel( namespaceId ).addPrimaryKey( emps.id, Collections.singletonList( getSnapshot().getRelSnapshot( namespaceId ).getColumn( emps.id, "empid" ).id ) );
        getLogicalRel( namespaceId ).addPrimaryKey( emp.id, Collections.singletonList( getSnapshot().getRelSnapshot( namespaceId ).getColumn( emp.id, "employeeno" ).id ) );
        getLogicalRel( namespaceId ).addPrimaryKey( work.id, Collections.singletonList( getSnapshot().getRelSnapshot( namespaceId ).getColumn( work.id, "employeeno" ).id ) );

        // set foreign keys
        getLogicalRel( namespaceId ).addForeignKey(
                emps.id,
                ImmutableList.of( getSnapshot().getRelSnapshot( namespaceId ).getColumn( emps.id, "deptno" ).id ),
                depts.id,
                ImmutableList.of( getSnapshot().getRelSnapshot( namespaceId ).getColumn( depts.id, "deptno" ).id ),
                "fk_emps_depts",
                ForeignKeyOption.NONE,
                ForeignKeyOption.NONE );
        getLogicalRel( namespaceId ).addForeignKey(
                work.id,
                ImmutableList.of( getSnapshot().getRelSnapshot( namespaceId ).getColumn( work.id, "employeeno" ).id ),
                emp.id,
                ImmutableList.of( getSnapshot().getRelSnapshot( namespaceId ).getColumn( emp.id, "employeeno" ).id ),
                "fk_work_emp",
                ForeignKeyOption.NONE,
                ForeignKeyOption.NONE );
    }


    private void addDefaultCsvColumn( CatalogAdapter csv, LogicalTable table, String name, PolyType type, Collation collation, int position, Integer length ) {
        if ( !getSnapshot().getRelSnapshot( table.namespaceId ).checkIfExistsColumn( table.id, name ) ) {
            long colId = getLogicalRel( table.namespaceId ).addColumn( name, table.id, position, type, null, length, null, null, null, false, collation );
            String filename = table.name + ".csv";
            if ( table.name.equals( "emp" ) || table.name.equals( "work" ) ) {
                filename += ".gz";
            }

            updateSnapshot();
            long allocId = 0;
            if ( !getSnapshot().getAllocSnapshot().adapterHasPlacement( csv.id, table.id ) ) {
                allocId = getAllocRel( table.namespaceId ).addDataPlacement( csv.id, table.id );
            } else {
                allocId = getSnapshot().getAllocSnapshot().getAllocation( csv.id, table.id ).id;
            }

            getAllocRel( table.namespaceId ).addColumnPlacement( allocId, colId, PlacementType.AUTOMATIC, filename, table.name, name, position );
            //getAllocRel( table.namespaceId ).updateColumnPlacementPhysicalPosition( allocId, colId, position );

            updateSnapshot();

            // long partitionId = table.partitionProperty.partitionIds.get( 0 );
            // getAllocRel( table.namespaceId ).addPartitionPlacement( table.namespaceId, csv.id, table.id, partitionId, PlacementType.AUTOMATIC, DataPlacementRole.UPTODATE );
        }
    }


    private void addDefaultColumn( CatalogAdapter adapter, LogicalTable table, String name, PolyType type, Collation collation, int position, Integer length ) {
        if ( !getSnapshot().getRelSnapshot( table.namespaceId ).checkIfExistsColumn( table.id, name ) ) {
            long colId = getLogicalRel( table.namespaceId ).addColumn( name, table.id, position, type, null, length, null, null, null, false, collation );
            AllocationEntity entity = getSnapshot().getAllocSnapshot().getAllocation( adapter.id, table.id );
            getAllocRel( table.namespaceId ).addColumnPlacement( entity.id, colId, PlacementType.AUTOMATIC, "col" + colId, table.name, name, position );
            getAllocRel( table.namespaceId ).updateColumnPlacementPhysicalPosition( adapter.id, colId, position );
        }
    }


    private void updateSnapshot() {
        // reset physical catalogs
        Set<Long> keys = this.physicalCatalogs.keySet();
        keys.forEach( k -> this.physicalCatalogs.replace( k, new PolyPhysicalCatalog() ) );

        // generate new physical entities, atm only relational
        this.allocationCatalogs.forEach( ( k, v ) -> {
            if ( v.getNamespace().namespaceType == NamespaceType.RELATIONAL ) {
                ((AllocationRelationalCatalog) v).getTables().forEach( ( k2, v2 ) -> {
                    LogicalTable table = getSnapshot().getLogicalEntity( v2.logicalId ).unwrap( LogicalTable.class );
                    List<PhysicalEntity> physicals = AdapterManager.getInstance().getAdapter( v2.adapterId ).createAdapterTable( idBuilder, table, v2 );
                    getPhysical( table.namespaceId ).addEntities( physicals );
                } );
            }
        } );

        this.snapshot = SnapshotBuilder.createSnapshot( idBuilder.getNewSnapshotId(), this, logicalCatalogs, allocationCatalogs, physicalCatalogs );
    }


    private void change() {
        // empty for now
    }


    public void commit() {
        log.debug( "commit" );
        updateSnapshot();
    }


    public void rollback() {
        log.debug( "rollback" );
    }


    private void validateNamespaceType( long id, NamespaceType type ) {
        if ( logicalCatalogs.get( id ).getLogicalNamespace().namespaceType != type ) {
            throw new RuntimeException( "error while retrieving catalog" );
        }
    }


    @Override
    public LogicalRelationalCatalog getLogicalRel( long namespaceId ) {
        validateNamespaceType( namespaceId, NamespaceType.RELATIONAL );
        return (LogicalRelationalCatalog) logicalCatalogs.get( namespaceId );
    }


    @Override
    public LogicalDocumentCatalog getLogicalDoc( long namespaceId ) {
        validateNamespaceType( namespaceId, NamespaceType.DOCUMENT );
        return (LogicalDocumentCatalog) logicalCatalogs.get( namespaceId );
    }


    @Override
    public LogicalGraphCatalog getLogicalGraph( long namespaceId ) {
        validateNamespaceType( namespaceId, NamespaceType.GRAPH );
        return (LogicalGraphCatalog) logicalCatalogs.get( namespaceId );
    }


    @Override
    public AllocationRelationalCatalog getAllocRel( long namespaceId ) {
        validateNamespaceType( namespaceId, NamespaceType.RELATIONAL );
        return (AllocationRelationalCatalog) allocationCatalogs.get( namespaceId );
    }


    @Override
    public AllocationDocumentCatalog getAllocDoc( long namespaceId ) {
        validateNamespaceType( namespaceId, NamespaceType.DOCUMENT );
        return (AllocationDocumentCatalog) allocationCatalogs.get( namespaceId );
    }


    @Override
    public AllocationGraphCatalog getAllocGraph( long namespaceId ) {
        validateNamespaceType( namespaceId, NamespaceType.GRAPH );
        return (AllocationGraphCatalog) allocationCatalogs.get( namespaceId );
    }


    @Override
    public PhysicalCatalog getPhysical( long namespaceId ) {
        return physicalCatalogs.get( namespaceId );
    }


    @Override
    @Deprecated
    public Map<Long, AlgNode> getNodeInfo() {
        return null;
    }


    @Override
    @Deprecated
    public void restoreInterfacesIfNecessary() {

    }


    @Override
    @Deprecated
    public void validateColumns() {

    }


    @Override
    @Deprecated
    public void restoreColumnPlacements( Transaction transaction ) {

    }


    @Override
    @Deprecated
    public void restoreViews( Transaction transaction ) {

    }


    @Override
    public long addUser( String name, String password ) {
        long id = idBuilder.getNewUserId();
        users.put( id, new CatalogUser( id, name, password ) );
        return id;
    }


    public long addNamespace( String name, NamespaceType namespaceType, boolean caseSensitive ) {
        long id = idBuilder.getNewNamespaceId();
        LogicalNamespace namespace = new LogicalNamespace( id, name, namespaceType, caseSensitive );

        switch ( namespaceType ) {
            case RELATIONAL:
                logicalCatalogs.put( id, new RelationalCatalog( namespace ) );
                allocationCatalogs.put( id, new PolyAllocRelCatalog( namespace ) );
                break;
            case DOCUMENT:
                logicalCatalogs.put( id, new DocumentCatalog( namespace ) );
                allocationCatalogs.put( id, new PolyAllocDocCatalog( namespace ) );
                break;
            case GRAPH:
                logicalCatalogs.put( id, new GraphCatalog( namespace ) );
                allocationCatalogs.put( id, new PolyAllocGraphCatalog( namespace ) );
                break;
        }
        physicalCatalogs.put( id, new PolyPhysicalCatalog() );
        change();
        return id;
    }


    @Override
    public void renameNamespace( long id, String name ) {
        if ( logicalCatalogs.get( id ) == null ) {
            return;
        }

        logicalCatalogs.put( id, logicalCatalogs.get( id ).withLogicalNamespace( logicalCatalogs.get( id ).getLogicalNamespace().withName( name ) ) );

        change();
    }


    @Override
    public void deleteNamespace( long id ) {
        logicalCatalogs.remove( id );

        change();
    }


    @Override
    public long addAdapter( String uniqueName, String clazz, AdapterType type, Map<String, String> settings ) {
        long id = idBuilder.getNewAdapterId();
        adapters.put( id, new CatalogAdapter( id, uniqueName, clazz, type, settings ) );
        return id;
    }


    @Override
    public void updateAdapterSettings( long adapterId, Map<String, String> newSettings ) {
        if ( !adapters.containsKey( adapterId ) ) {
            return;
        }
        adapters.put( adapterId, adapters.get( adapterId ).toBuilder().settings( ImmutableMap.copyOf( newSettings ) ).build() );
    }


    @Override
    public void deleteAdapter( long id ) {
        adapters.remove( id );
    }


    @Override
    public long addQueryInterface( String uniqueName, String clazz, Map<String, String> settings ) {
        long id = idBuilder.getNewInterfaceId();

        interfaces.put( id, new CatalogQueryInterface( id, uniqueName, clazz, settings ) );

        return id;
    }


    @Override
    public void deleteQueryInterface( long id ) {
        interfaces.remove( id );
    }


    @Override
    public void close() {
        log.error( "closing" );
    }


    @Override
    public void clear() {
        log.error( "clearing" );
    }


    @Override
    public PolyCatalog copy() {
        return deserialize( serialize(), PolyCatalog.class );
    }

}

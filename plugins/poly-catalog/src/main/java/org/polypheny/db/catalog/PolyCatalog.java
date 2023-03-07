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
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
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
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogQueryInterface;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownQueryInterfaceException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.catalog.logical.DocumentCatalog;
import org.polypheny.db.catalog.logical.GraphCatalog;
import org.polypheny.db.catalog.logical.RelationalCatalog;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.ForeignKeyOption;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.catalog.physical.PolyPhysicalCatalog;
import org.polypheny.db.catalog.snapshot.FullSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;


/**
 * Central catalog, which distributes the operations to the corresponding model catalogs.
 * Object are as follows:
 * Namespace -> Schema (Relational), Graph (Graph), Database (Document)
 * Entity -> Table (Relational), does not exist (Graph), Collection (Document)
 * Field -> Column (Relational), does not exist (Graph), Field (Document)
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
    public final Map<Long, CatalogUser> users;

    @Serialize
    public final Map<Long, CatalogAdapter> adapters;

    @Serialize
    public final Map<Long, CatalogQueryInterface> interfaces;

    private final IdBuilder idBuilder = IdBuilder.getInstance();
    private FullSnapshot fullSnapshot;

    protected final PropertyChangeSupport listeners = new PropertyChangeSupport( this );


    public PolyCatalog() {
        this(
                new ConcurrentHashMap<>(),
                new ConcurrentHashMap<>(),
                new ConcurrentHashMap<>(),
                new ConcurrentHashMap<>(),
                new ConcurrentHashMap<>(),
                new ConcurrentHashMap<>() );

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
            addAdapter( "hsqldb", defaultStore.getAdapterName(), AdapterType.STORE, defaultStore.getDefaultSettings() );

            // Deploy default CSV view
            long adapter = addAdapter( "hr", defaultSource.getAdapterName(), AdapterType.SOURCE, defaultSource.getDefaultSettings() );

            // init schema
            CatalogAdapter csv = getAdapter( "hr" );

            long id = getLogicalRel( namespaceId ).addTable( "depts", EntityType.SOURCE, false );

            id = getLogicalRel( namespaceId ).addTable( "emps", EntityType.SOURCE, false );

            id = getLogicalRel( namespaceId ).addTable( "emp", EntityType.SOURCE, false );

            id = getLogicalRel( namespaceId ).addTable( "work", EntityType.SOURCE, false );
            try {
                addDefaultCsvColumns( csv );
            } catch ( UnknownTableException | GenericCatalogException | UnknownColumnException e ) {
                throw new RuntimeException( e );
            }


        }

        commit();

    }


    /**
     * Initiates default columns for csv files
     */
    private void addDefaultCsvColumns( CatalogAdapter csv ) throws UnknownTableException, GenericCatalogException, UnknownColumnException, UnknownTableException, UnknownColumnException, GenericCatalogException {
        LogicalNamespace schema = getNamespace( "public" );
        LogicalTable depts = getLogicalRel( schema.id ).getTable( "depts" );

        addDefaultCsvColumn( csv, depts, "deptno", PolyType.INTEGER, null, 1, null );
        addDefaultCsvColumn( csv, depts, "name", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 2, 20 );

        LogicalTable emps = getLogicalRel( schema.id ).getTable( "emps" );
        addDefaultCsvColumn( csv, emps, "empid", PolyType.INTEGER, null, 1, null );
        addDefaultCsvColumn( csv, emps, "deptno", PolyType.INTEGER, null, 2, null );
        addDefaultCsvColumn( csv, emps, "name", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 3, 20 );
        addDefaultCsvColumn( csv, emps, "salary", PolyType.INTEGER, null, 4, null );
        addDefaultCsvColumn( csv, emps, "commission", PolyType.INTEGER, null, 5, null );

        LogicalTable emp = getLogicalRel( schema.id ).getTable( "emp" );
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

        LogicalTable work = getLogicalRel( schema.id ).getTable( "work" );
        addDefaultCsvColumn( csv, work, "employeeno", PolyType.INTEGER, null, 1, null );
        addDefaultCsvColumn( csv, work, "educationfield", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 2, 20 );
        addDefaultCsvColumn( csv, work, "jobinvolvement", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 3, 20 );
        addDefaultCsvColumn( csv, work, "joblevel", PolyType.INTEGER, null, 4, null );
        addDefaultCsvColumn( csv, work, "jobrole", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 5, 30 );
        addDefaultCsvColumn( csv, work, "businesstravel", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 6, 20 );
        addDefaultCsvColumn( csv, work, "department", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 7, 25 );
        addDefaultCsvColumn( csv, work, "attrition", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 8, 20 );
        addDefaultCsvColumn( csv, work, "dailyrate", PolyType.INTEGER, null, 9, null );

        // set all needed primary keys
        getLogicalRel( schema.id ).addPrimaryKey( depts.id, Collections.singletonList( getLogicalRel( schema.id ).getColumn( depts.id, "deptno" ).id ) );
        getLogicalRel( schema.id ).addPrimaryKey( emps.id, Collections.singletonList( getLogicalRel( schema.id ).getColumn( emps.id, "empid" ).id ) );
        getLogicalRel( schema.id ).addPrimaryKey( emp.id, Collections.singletonList( getLogicalRel( schema.id ).getColumn( emp.id, "employeeno" ).id ) );
        getLogicalRel( schema.id ).addPrimaryKey( work.id, Collections.singletonList( getLogicalRel( schema.id ).getColumn( work.id, "employeeno" ).id ) );

        // set foreign keys
        getLogicalRel( schema.id ).addForeignKey(
                emps.id,
                ImmutableList.of( getLogicalRel( schema.id ).getColumn( emps.id, "deptno" ).id ),
                depts.id,
                ImmutableList.of( getLogicalRel( schema.id ).getColumn( depts.id, "deptno" ).id ),
                "fk_emps_depts",
                ForeignKeyOption.NONE,
                ForeignKeyOption.NONE );
        getLogicalRel( schema.id ).addForeignKey(
                work.id,
                ImmutableList.of( getLogicalRel( schema.id ).getColumn( work.id, "employeeno" ).id ),
                emp.id,
                ImmutableList.of( getLogicalRel( schema.id ).getColumn( emp.id, "employeeno" ).id ),
                "fk_work_emp",
                ForeignKeyOption.NONE,
                ForeignKeyOption.NONE );
    }


    private void addDefaultCsvColumn( CatalogAdapter csv, LogicalTable table, String name, PolyType type, Collation collation, int position, Integer length ) {
        if ( !getLogicalRel( table.namespaceId ).checkIfExistsColumn( table.id, name ) ) {
            long colId = getLogicalRel( table.namespaceId ).addColumn( name, table.id, position, type, null, length, null, null, null, false, collation );
            String filename = table.name + ".csv";
            if ( table.name.equals( "emp" ) || table.name.equals( "work" ) ) {
                filename += ".gz";
            }

            getAllocRel( table.namespaceId ).addColumnPlacement( table, csv.id, colId, PlacementType.AUTOMATIC, filename, table.name, name, position );
            getAllocRel( table.namespaceId ).updateColumnPlacementPhysicalPosition( csv.id, colId, position );

            // long partitionId = table.partitionProperty.partitionIds.get( 0 );
            // getAllocRel( table.namespaceId ).addPartitionPlacement( table.namespaceId, csv.id, table.id, partitionId, PlacementType.AUTOMATIC, DataPlacementRole.UPTODATE );
        }
    }


    private void addDefaultColumn( CatalogAdapter adapter, LogicalTable table, String name, PolyType type, Collation collation, int position, Integer length ) {
        if ( !getLogicalRel( table.namespaceId ).checkIfExistsColumn( table.id, name ) ) {
            long colId = getLogicalRel( table.namespaceId ).addColumn( name, table.id, position, type, null, length, null, null, null, false, collation );
            getAllocRel( table.namespaceId ).addColumnPlacement( table, adapter.id, colId, PlacementType.AUTOMATIC, "col" + colId, table.name, name, position );
            getAllocRel( table.namespaceId ).updateColumnPlacementPhysicalPosition( adapter.id, colId, position );
        }
    }


    private void updateSnapshot() {
        this.fullSnapshot = new FullSnapshot( idBuilder.getNewSnapshotId(), logicalCatalogs );
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
    public LogicalEntity getLogicalEntity( String entityName ) {
        for ( LogicalCatalog catalog : logicalCatalogs.values() ) {
            LogicalEntity entity = catalog.getEntity( entityName );
            if ( entity != null ) {
                return entity;
            }
        }
        return null;
    }


    @Override
    public LogicalEntity getLogicalEntity( long id ) {
        for ( LogicalCatalog catalog : logicalCatalogs.values() ) {
            LogicalEntity entity = catalog.getEntity( id );
            if ( entity != null ) {
                return entity;
            }
        }
        return null;
    }


    @Override
    public PhysicalCatalog getPhysical( long namespaceId ) {
        return physicalCatalogs.get( namespaceId );
    }


    // move to Snapshot
    @Override
    public PhysicalEntity<?> getPhysicalEntity( long id ) {
        for ( PhysicalCatalog catalog : physicalCatalogs.values() ) {
            PhysicalEntity<?> entity = catalog.getPhysicalEntity( id );
            if ( entity != null ) {
                return entity;
            }
        }
        return null;
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
                allocationCatalogs.put( id, new PolyAllocRelCatalog() );
                break;
            case DOCUMENT:
                logicalCatalogs.put( id, new DocumentCatalog( namespace ) );
                allocationCatalogs.put( id, new PolyAllocDocCatalog() );
                break;
            case GRAPH:
                logicalCatalogs.put( id, new GraphCatalog( namespace ) );
                allocationCatalogs.put( id, new PolyAllocGraphCatalog() );
                break;
        }
        physicalCatalogs.put( id, new PolyPhysicalCatalog() );
        change();
        return id;
    }


    @Override
    public @NonNull List<LogicalNamespace> getNamespaces( Pattern name ) {
        if ( name == null ) {
            return logicalCatalogs.values().stream().map( LogicalCatalog::getLogicalNamespace ).collect( Collectors.toList() );
        }

        return logicalCatalogs.values().stream().filter( c ->
                        c.getLogicalNamespace().caseSensitive
                                ? c.getLogicalNamespace().name.toLowerCase( Locale.ROOT ).matches( name.pattern )
                                : c.getLogicalNamespace().name.matches( name.pattern ) )
                .map( LogicalCatalog::getLogicalNamespace ).collect( Collectors.toList() );
    }


    @Override
    public LogicalNamespace getNamespace( long id ) {
        return logicalCatalogs.get( id ).getLogicalNamespace();
    }


    @Override
    public LogicalNamespace getNamespace( String name ) {
        List<LogicalNamespace> namespaces = getNamespaces( Pattern.of( name ) );
        if ( namespaces.isEmpty() ) {
            return null;
        } else if ( namespaces.size() > 1 ) {
            throw new RuntimeException( "multiple namespaces retrieved" );
        }
        return namespaces.get( 0 );

    }


    @Override
    public boolean checkIfExistsNamespace( String name ) {
        return !getNamespaces( Pattern.of( name ) ).isEmpty();
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
    public CatalogUser getUser( String name ) throws UnknownUserException {
        return users.values().stream().filter( u -> u.name.equals( name ) ).findFirst().orElse( null );
    }


    @Override
    public CatalogUser getUser( long id ) {
        return users.get( id );
    }


    @Override
    public List<CatalogAdapter> getAdapters() {
        return List.copyOf( adapters.values() );
    }


    @Override
    public CatalogAdapter getAdapter( String uniqueName ) throws UnknownAdapterException {
        return adapters.values().stream().filter( a -> a.uniqueName.equals( uniqueName ) ).findFirst().orElse( null );
    }


    @Override
    public CatalogAdapter getAdapter( long id ) {
        return adapters.get( id );
    }


    @Override
    public boolean checkIfExistsAdapter( long id ) {
        return adapters.containsKey( id );
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
    public List<CatalogQueryInterface> getQueryInterfaces() {
        return List.copyOf( interfaces.values() );
    }


    @Override
    public CatalogQueryInterface getQueryInterface( String uniqueName ) throws UnknownQueryInterfaceException {
        return interfaces.values().stream().filter( i -> i.name.equals( uniqueName ) ).findFirst().orElse( null );
    }


    @Override
    public CatalogQueryInterface getQueryInterface( long id ) {
        return interfaces.get( id );
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
    public Snapshot getSnapshot( long id ) {
        return null;
    }


    @Override
    public List<AllocationEntity<?>> getAllocationsOnAdapter( long id ) {
        return allocationCatalogs.values().stream().flatMap( c -> c.getAllocationsOnAdapter( id ).stream() ).collect( Collectors.toList() );
    }


    @Override
    public List<PhysicalEntity<?>> getPhysicalsOnAdapter( long id ) {
        return physicalCatalogs.values().stream().flatMap( c -> c.getPhysicalsOnAdapter( id ).stream() ).collect( Collectors.toList() );
    }


    @Override
    public List<CatalogIndex> getIndexes() {
        return List.of();
    }


    @Override
    public List<LogicalTable> getTablesForPeriodicProcessing() {
        return List.of();
    }


    @Override
    public PolyCatalog copy() {
        return deserialize( serialize(), PolyCatalog.class );
    }

}

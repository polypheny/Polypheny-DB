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

package org.polypheny.db.workflow.dag.activities.impl;

import static org.polypheny.db.workflow.dag.activities.impl.RelLoadActivity.TABLE_KEY;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.ConstraintType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.DdlManager.ConstraintInformation;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.locking.Lockable.LockType;
import org.polypheny.db.transaction.locking.LockablesRegistry;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.DefaultGroup;
import org.polypheny.db.workflow.dag.annotations.EntitySetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.annotations.Group.Subgroup;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.EntityValue;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringSettingDef.AutoCompleteType;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.BatchWriter;
import org.polypheny.db.workflow.engine.storage.QueryUtils;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.StorageManagerImpl;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.RelReader;

@ActivityDefinition(type = "relLoad", displayName = "Load Table to Polypheny", categories = { ActivityCategory.LOAD, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.REL) },
        outPorts = {},
        shortDescription = "Loads the table into a target table within this Polypheny instance.")
@DefaultGroup(subgroups = { @Subgroup(key = "create", displayName = "Create Table") })

@EntitySetting(key = TABLE_KEY, displayName = "Table", dataModel = DataModel.RELATIONAL, pos = 0)
@BoolSetting(key = "drop", displayName = "Truncate Existing Table", pos = 1,
        shortDescription = "Delete any rows that are already in the specified table.", defaultValue = false)
@BoolSetting(key = "keepPk", displayName = "Keep Primary Key", pos = 2,
        shortDescription = "Use the '" + StorageManager.PK_COL + "' column as primary key.", defaultValue = false)

@BoolSetting(key = "create", displayName = "Create if Missing", pos = 0, subGroup = "create",
        shortDescription = "Create a new table with the specified name, if it does not yet exist.")
@StringSetting(key = "adapter", displayName = "Adapter", shortDescription = "Specify which adapter is used when a new table is created.", subGroup = "create",
        subPointer = "create", subValues = { "true" }, pos = 1, autoCompleteType = AutoCompleteType.ADAPTERS)
@FieldSelectSetting(key = "pkCols", displayName = "Primary Key Column(s)", pos = 2,
        shortDescription = "The column(s) to be used as primary key(s). Only has an effect if the primary key is not kept.", simplified = true, reorder = true, targetInput = 0,
        subPointer = "create", subValues = { "true" }, subGroup = "create")

@SuppressWarnings("unused")
public class RelLoadActivity implements Activity, Pipeable {

    static final String TABLE_KEY = "table";
    private final AdapterManager adapterManager = AdapterManager.getInstance();
    private final DdlManager ddlManager = DdlManager.getInstance();


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( settings.keysPresent( "create", "adapter" ) && settings.getBool( "create" ) ) {
            String adapter = settings.getString( "adapter" );
            adapterManager.getStore( adapter ).orElseThrow( () -> new InvalidSettingException( "Adapter does not exist: " + adapter, "adapter" ) );
        }
        return List.of();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        LogicalTable table = getEntity( settings, inputs.get( 0 ).getTupleType(), ctx::getTransaction, ctx::logInfo );
        write( table, ctx.getTransaction(), inputs.get( 0 ).getIterable(), ctx, null,
                ((RelReader) inputs.get( 0 )).getRowCount(), settings.getBool( "keepPk" ) );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return null;
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        long estimatedTupleCount = estimateTupleCount( inputs.stream().map( InputPipe::getType ).toList(), settings, ctx.getEstimatedInCounts(), ctx::getTransaction );

        LogicalTable table = getEntity( settings, inputs.get( 0 ).getType(), ctx::getTransaction, ctx::logInfo );
        write( table, ctx.getTransaction(), inputs.get( 0 ), null, ctx, estimatedTupleCount, settings.getBool( "keepPk" ) ); // we do not know the number of rows
    }


    @Override
    public String getDynamicName( List<TypePreview> inTypes, SettingsPreview settings ) {
        Optional<EntityValue> table = settings.get( TABLE_KEY, EntityValue.class );
        return table.map( v -> String.format( "Load Table to '%s.%s'", v.getNamespace(), v.getName() ) ).orElse( null );
    }


    private LogicalTable getEntity( Settings settings, AlgDataType inType, Supplier<Transaction> txSupplier, Consumer<String> logInfo ) throws ActivityException {
        // TODO: check if the adapter is a data store (and thus writable)
        EntityValue setting = settings.get( TABLE_KEY, EntityValue.class );
        LogicalTable table = setting.getTable();
        boolean create = settings.getBool( "create" );
        boolean drop = settings.getBool( "drop" );
        String adapter = settings.getString( "adapter" );
        boolean keepPk = settings.getBool( "keepPk" );
        List<String> pkCols = settings.get( "pkCols", FieldSelectValue.class ).getInclude();
        AlgDataType outType = getOutType( inType, keepPk );

        if ( table == null ) {
            if ( !create ) {
                throw new InvalidSettingException( "Specified table does not exist", TABLE_KEY );
            }
            LogicalNamespace namespace = setting.getLogicalNamespace();
            if ( namespace == null ) {
                logInfo.accept( "Creating namespace '" + setting.getNamespace() + "'" );
                try {
                    long namespaceId = ddlManager.createNamespace( setting.getNamespace(), DataModel.RELATIONAL, false, false, false, null );
                    namespace = Catalog.getInstance().getSnapshot().getNamespace( namespaceId ).orElse( null );
                } catch ( Exception e ) {
                    throw new InvalidSettingException( "Specified namespace cannot be created: " + e.getMessage(), TABLE_KEY );
                }
            }
            // TODO: writing to a created table currently does not work.
            logInfo.accept( "Creating table '" + setting.getName() + "'" );
            assert namespace != null;
            // Unfortunately, we have to create the table outside our activity transaction context.
            Transaction transaction = QueryUtils.startTransaction( namespace.id, "RelLoadCreate" );
            try {
                transaction.acquireLockable( LockablesRegistry.INSTANCE.getOrCreateLockable( namespace ), LockType.SHARED ); // SHARED lock is workaround to be able to create entity while other txs are reading
                ddlManager.createTable(
                        namespace.id,
                        setting.getName(),
                        StorageManagerImpl.getFieldInfo( outType ),
                        getPkConstraint( keepPk, pkCols ),
                        false,
                        List.of( adapterManager.getStore( adapter ).orElseThrow( () ->
                                new InvalidSettingException( "Adapter does not exist: " + adapter, "adapter" ) ) ),
                        PlacementType.AUTOMATIC,
                        transaction.createStatement() );
                transaction.commit();
                return Catalog.snapshot().rel().getTable( namespace.id, setting.getName() ).orElseThrow();
            } finally {
                if ( transaction.isActive() ) {
                    logInfo.accept( "Unable to create table. Rolling back transaction." );
                    transaction.rollback( null );
                }
            }

        } else {
            if ( !ActivityUtils.areTypesCompatible( List.of( outType, table.getTupleType() ) ) ) {
                throw new InvalidSettingException( "Target table is incompatible with the input table", TABLE_KEY );
            }

            if ( drop ) {
                logInfo.accept( "Truncating existing table" );
                Transaction transaction = txSupplier.get();
                Statement statement = transaction.createStatement();
                List<AllocationEntity> allocations = transaction.getSnapshot().alloc().getFromLogical( table.id );
                allocations.forEach( a -> AdapterManager.getInstance().getAdapter( a.adapterId ).orElseThrow().truncate( statement.getPrepareContext(), a.id ) );
            }
        }
        return table;
    }


    private void write(
            LogicalTable table,
            Transaction transaction,
            Iterable<List<PolyValue>> rows,
            ExecutionContext ctx,
            PipeExecutionContext pipeCtx,
            long totalRows,
            boolean keepPkCol ) throws Exception {
        assert ctx != null || pipeCtx != null;

        Map<Long, AlgDataType> paramTypes = new HashMap<>();
        StringJoiner joiner = new StringJoiner( ", ", "(", ")" );
        for ( int i = 0; i < table.getTupleType().getFieldCount(); i++ ) {
            joiner.add( "?" );
            AlgDataType fieldType = table.getTupleType().getFields().get( i ).getType();
            paramTypes.put( (long) i, fieldType );
        }

        String query = "INSERT INTO " + QueryUtils.quotedIdentifier( table ) + " VALUES " + joiner;
        QueryContext context = QueryUtils.constructContext( query, "SQL", table.getNamespaceId(), transaction );

        int mapCapacity = (int) Math.ceil( table.getTupleType().getFieldCount() / 0.75 );
        long rowCount = 0;
        long countDelta = Math.max( totalRows / 100, 1 );
        int startIdx = keepPkCol ? 0 : 1;
        try ( BatchWriter writer = new BatchWriter( context, transaction.createStatement(), paramTypes ) ) {
            for ( List<PolyValue> row : rows ) {
                Map<Long, PolyValue> map = new HashMap<>( mapCapacity );
                if ( keepPkCol ) {
                    map.put( 0L, PolyLong.of( rowCount ) );
                    for ( int i = 1; i < row.size(); i++ ) { // skip primary key
                        map.put( (long) i, row.get( i ) );
                    }
                } else {
                    for ( int i = 1; i < row.size(); i++ ) { // skip primary key
                        map.put( (long) i - 1, row.get( i ) );
                    }
                }
                writer.write( map );

                rowCount++;

                if ( rowCount % countDelta == 0 ) {
                    double progress = (double) rowCount / totalRows;
                    if ( ctx != null ) {
                        ctx.updateProgress( progress );
                        ctx.checkInterrupted();
                    } else {
                        pipeCtx.updateProgress( progress );
                    }
                }
            }
        }
    }


    private AlgDataType getOutType( AlgDataType inType, boolean keepPkCol ) {
        if ( keepPkCol ) {
            return inType;
        }
        return ActivityUtils.removeField( inType, StorageManager.PK_COL );
    }


    private List<ConstraintInformation> getPkConstraint( boolean keepPkCol, List<String> pkCols ) {
        List<String> cols = keepPkCol ? List.of( StorageManager.PK_COL ) : pkCols;
        return List.of( new ConstraintInformation( "PRIMARY KEY", ConstraintType.PRIMARY, cols ) );
    }

}

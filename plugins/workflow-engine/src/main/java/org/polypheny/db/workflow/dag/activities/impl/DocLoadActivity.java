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

import static org.polypheny.db.workflow.dag.activities.impl.DocLoadActivity.COLL_KEY;

import java.util.List;
import java.util.function.Supplier;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.locking.Lockable.LockType;
import org.polypheny.db.transaction.locking.LockablesRegistry;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.EntitySetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.BoolValue;
import org.polypheny.db.workflow.dag.settings.EntityValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringSettingDef.AutoCompleteType;
import org.polypheny.db.workflow.dag.settings.StringValue;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.DocBatchWriter;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.DocReader;

@ActivityDefinition(type = "docLoad", displayName = "Load Collection to Polypheny", categories = { ActivityCategory.LOAD, ActivityCategory.DOCUMENT },
        inPorts = { @InPort(type = PortType.DOC) },
        outPorts = {})

@EntitySetting(key = COLL_KEY, displayName = "Collection", dataModel = DataModel.DOCUMENT)
@BoolSetting(key = "create", displayName = "Create Collection",
        shortDescription = "Create a new collection with the specified name, if it does not yet exist. The namespace must exist in any case.")
@StringSetting(key = "adapter", displayName = "Adapter", shortDescription = "Specify which adapter is used when a new collection is created",
        subPointer = "create", subValues = { "true" }, autoCompleteType = AutoCompleteType.ADAPTERS)
// TODO: delete existing docs BoolSetting
@SuppressWarnings("unused")
public class DocLoadActivity implements Activity, Pipeable {

    static final String COLL_KEY = "collection";
    private final AdapterManager adapterManager = AdapterManager.getInstance();
    private final DdlManager ddlManager = DdlManager.getInstance();


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( settings.keysPresent( "create", "adapter" ) && settings.getOrThrow( "create", BoolValue.class ).getValue() ) {
            String adapter = settings.getOrThrow( "adapter", StringValue.class ).getValue();
            adapterManager.getStore( adapter ).orElseThrow( () -> new InvalidSettingException( "Adapter does not exist: " + adapter, "adapter" ) );
        }
        return List.of();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        LogicalCollection collection = getEntity( settings.get( COLL_KEY, EntityValue.class ),
                settings.get( "create", BoolValue.class ).getValue(),
                settings.get( "adapter", StringValue.class ).getValue(),
                ctx::getTransaction );
        DocReader reader = (DocReader) inputs.get( 0 );
        write( collection, ctx.getTransaction(), reader.getIterable(), ctx, null, reader.getDocCount() );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return null;
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        long estimatedTupleCount = estimateTupleCount( inputs.stream().map( InputPipe::getType ).toList(), settings, ctx.getEstimatedInCounts(), ctx::getTransaction );

        LogicalCollection collection = getEntity( settings.get( COLL_KEY, EntityValue.class ),
                settings.get( "create", BoolValue.class ).getValue(),
                settings.get( "adapter", StringValue.class ).getValue(),
                ctx::getTransaction );
        write( collection, ctx.getTransaction(), inputs.get( 0 ), null, ctx, estimatedTupleCount );
    }


    private LogicalCollection getEntity(
            EntityValue setting,
            boolean canCreate,
            String adapter,
            Supplier<Transaction> txSupplier ) throws ActivityException {
        // TODO: check if the adapter is a data store (and thus writable)
        LogicalCollection collection = setting.getCollection();
        if ( collection == null ) {
            if ( !canCreate ) {
                throw new InvalidSettingException( "Specified collection does not exist", COLL_KEY );
            }
            LogicalNamespace namespace = setting.getLogicalNamespace();
            if ( namespace == null ) {
                throw new InvalidSettingException( "Specified namespace does not exist", COLL_KEY );
            }
            // no need for try catch: transaction is rolled back by WorkflowScheduler if an exception is thrown
            Transaction transaction = txSupplier.get();
            transaction.acquireLockable( LockablesRegistry.INSTANCE.getOrCreateLockable( namespace ), LockType.EXCLUSIVE );
            ddlManager.createCollection(
                    namespace.getId(),
                    setting.getName(),
                    false,
                    List.of( adapterManager.getStore( adapter ).orElseThrow( () ->
                            new InvalidSettingException( "Adapter does not exist: " + adapter, "adapter" ) ) ),
                    PlacementType.AUTOMATIC,
                    transaction.createStatement()
            );
            return Catalog.snapshot().doc().getCollection( namespace.id, setting.getName() ).orElseThrow();

        }
        return collection;
    }


    private void write( LogicalCollection collection, Transaction transaction, Iterable<List<PolyValue>> tuples, ExecutionContext ctx, PipeExecutionContext pipeCtx, long totalTuples ) throws Exception {
        assert ctx != null || pipeCtx != null;

        long count = 0;
        long countDelta = Math.max( totalTuples / 100, 1 );
        try ( DocBatchWriter writer = new DocBatchWriter( collection, transaction ) ) {
            for ( List<PolyValue> tuple : tuples ) {
                writer.write( tuple.get( 0 ).asDocument() );

                count++;
                if ( count % countDelta == 0 ) {
                    double progress = (double) count / totalTuples;
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

}

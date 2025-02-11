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

import static org.polypheny.db.workflow.dag.activities.impl.LpgLoadActivity.GRAPH_KEY;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
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
import org.polypheny.db.workflow.dag.settings.EntityValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringSettingDef.AutoCompleteType;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.LpgBatchWriter;
import org.polypheny.db.workflow.engine.storage.QueryUtils;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.LpgReader;

@ActivityDefinition(type = "lpgLoad", displayName = "Load Graph to Polypheny", categories = { ActivityCategory.LOAD, ActivityCategory.GRAPH },
        inPorts = { @InPort(type = PortType.LPG) },
        outPorts = {},
        shortDescription = "Loads the graph into a target graph within this Polypheny instance.")

@EntitySetting(key = GRAPH_KEY, displayName = "Graph", dataModel = DataModel.GRAPH, pos = 0)
@BoolSetting(key = "drop", displayName = "Drop Existing Graph", pos = 1,
        shortDescription = "Drop any nodes that are already in the specified graph.", defaultValue = false)
@BoolSetting(key = "create", displayName = "Create Graph", pos = 2,
        shortDescription = "Create a new graph with the specified name, if it does not yet exist.")
@StringSetting(key = "adapter", displayName = "Adapter", shortDescription = "Specify which adapter is used when a new graph is created.",
        subPointer = "create", subValues = { "true" }, pos = 3, autoCompleteType = AutoCompleteType.ADAPTERS)
@SuppressWarnings("unused")
public class LpgLoadActivity implements Activity, Pipeable {

    public static final String GRAPH_KEY = "graph";
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
        LogicalGraph graph = getEntity( settings, ctx::getTransaction, ctx::logInfo );
        LpgReader reader = (LpgReader) inputs.get( 0 );
        write( graph, ctx.getTransaction(), reader.getIterable(), ctx, null, reader.getTupleCount() );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return null;
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        long estimatedTupleCount = estimateTupleCount( inputs.stream().map( InputPipe::getType ).toList(), settings, ctx.getEstimatedInCounts(), ctx::getTransaction );
        LogicalGraph graph = getEntity( settings, ctx::getTransaction, ctx::logInfo );
        write( graph, ctx.getTransaction(), inputs.get( 0 ), null, ctx, estimatedTupleCount );
    }


    private LogicalGraph getEntity( Settings settings, Supplier<Transaction> txSupplier, Consumer<String> logInfo ) throws ActivityException {
        // TODO: check if the adapter is a data store (and thus writable)
        EntityValue entitySetting = settings.get( GRAPH_KEY, EntityValue.class );
        LogicalGraph graph = entitySetting.getGraph();
        boolean create = settings.getBool( "create" );
        boolean drop = settings.getBool( "drop" );
        String adapter = settings.getString( "adapter" );

        if ( graph == null ) {
            if ( !create ) {
                throw new InvalidSettingException( "Specified graph does not exist", GRAPH_KEY );
            }
            logInfo.accept( "Creating graph '" + entitySetting.getNamespace() + "'" );

            // Unfortunately, we have to create the table outside our activity transaction context.
            Transaction transaction = QueryUtils.startTransaction( Catalog.defaultNamespaceId, "LpgLoadCreate" );
            try {
                long graphId = ddlManager.createGraph(
                        entitySetting.getNamespace(),
                        true,
                        List.of( adapterManager.getStore( adapter ).orElseThrow( () ->
                                new InvalidSettingException( "Adapter does not exist: " + adapter, "adapter" ) ) ),
                        false,
                        false,
                        RuntimeConfig.GRAPH_NAMESPACE_DEFAULT_CASE_SENSITIVE.getBoolean(),
                        transaction.createStatement()
                );
                transaction.commit();
                return Catalog.snapshot().graph().getGraph( graphId ).orElseThrow();
            } finally {
                if ( transaction.isActive() ) {
                    logInfo.accept( "Unable to create graph. Rolling back transaction." );
                    transaction.rollback( null );
                }
            }
        } else if ( drop ) {
            logInfo.accept( "Truncating existing graph" );
            Transaction transaction = txSupplier.get();
            Statement statement = transaction.createStatement();
            List<AllocationEntity> allocations = transaction.getSnapshot().alloc().getFromLogical( graph.id );
            allocations.forEach( a -> AdapterManager.getInstance().getAdapter( a.adapterId ).orElseThrow().truncate( statement.getPrepareContext(), a.id ) );
        }
        return graph;
    }


    private void write( LogicalGraph graph, Transaction transaction, Iterable<List<PolyValue>> tuples, ExecutionContext ctx, PipeExecutionContext pipeCtx, long totalTuples ) throws Exception {
        assert ctx != null || pipeCtx != null;

        long count = 0;
        long countDelta = Math.max( totalTuples / 100, 1 );
        try ( LpgBatchWriter writer = new LpgBatchWriter( graph, transaction ) ) {
            for ( List<PolyValue> tuple : tuples ) {
                PolyValue value = tuple.get( 0 );
                if ( value.isNode() ) {
                    writer.write( value.asNode() );
                } else if ( value.isEdge() ) {
                    writer.write( value.asEdge() );
                } else {
                    throw new IllegalArgumentException( "Cannot load graph data that does not represent a node or an edge" );
                }

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

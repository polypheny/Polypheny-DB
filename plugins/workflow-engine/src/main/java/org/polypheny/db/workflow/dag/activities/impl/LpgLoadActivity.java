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
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.logistic.DataModel;
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
import org.polypheny.db.workflow.dag.annotations.EntitySetting;
import org.polypheny.db.workflow.dag.settings.EntityValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.LpgBatchWriter;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.LpgReader;

@ActivityDefinition(type = "lpgLoad", displayName = "Load Graph to Polypheny", categories = { ActivityCategory.LOAD, ActivityCategory.GRAPH },
        inPorts = { @InPort(type = PortType.LPG) },
        outPorts = {})

@EntitySetting(key = GRAPH_KEY, displayName = "Graph", dataModel = DataModel.GRAPH)
@SuppressWarnings("unused")
public class LpgLoadActivity implements Activity, Pipeable {

    public static final String GRAPH_KEY = "graph";


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return List.of();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        LogicalGraph graph = getEntity( settings.get( GRAPH_KEY, EntityValue.class ) );
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
        LogicalGraph graph = getEntity( settings.get( GRAPH_KEY, EntityValue.class ) );
        write( graph, ctx.getTransaction(), inputs.get( 0 ), null, ctx, estimatedTupleCount );
    }


    private LogicalGraph getEntity( EntityValue setting ) throws ActivityException {
        // TODO: check if the adapter is a data store (and thus writable)
        LogicalGraph graph = setting.getGraph();
        if ( graph == null ) {
            throw new InvalidSettingException( "Specified graph does not exist", GRAPH_KEY );
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

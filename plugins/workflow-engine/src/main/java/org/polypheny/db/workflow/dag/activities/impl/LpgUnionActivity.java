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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.LpgType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.LpgInputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.LpgReader;
import org.polypheny.db.workflow.engine.storage.writer.LpgWriter;

@ActivityDefinition(type = "lpgUnion", displayName = "Graph Union", categories = { ActivityCategory.TRANSFORM, ActivityCategory.GRAPH },
        inPorts = { @InPort(type = PortType.LPG, description = "A single graph."), @InPort(type = PortType.LPG, isMulti = true, description = "One or more graphs.") },
        outPorts = { @OutPort(type = PortType.LPG) },
        shortDescription = "Combines the nodes and edges of all input graphs into a single graph."
)

@SuppressWarnings("unused")
public class LpgUnionActivity implements Activity, Pipeable {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        Set<String> nodes = new HashSet<>();
        Set<String> edges = new HashSet<>();
        for ( TypePreview preview : inTypes ) {
            if ( preview instanceof LpgType type ) {
                nodes.addAll( type.getKnownNodeLabels() );
                edges.addAll( type.getKnownEdgeLabels() );
            }
        }
        return LpgType.of( nodes, edges ).asOutTypes();
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getGraphType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        List<LpgInputPipe> graphs = inputs.stream().map( InputPipe::asLpgInputPipe ).toList();
        for ( LpgInputPipe graph : graphs ) {
            for ( PolyNode node : graph.getNodeIterable() ) {
                if ( !output.put( node ) ) {
                    inputs.forEach( InputPipe::finishIteration );
                    return;
                }
            }
        }
        for ( LpgInputPipe graph : graphs ) {
            for ( PolyEdge edge : graph.getEdgeIterable() ) {
                if ( !output.put( edge ) ) {
                    inputs.forEach( InputPipe::finishIteration );
                    return;
                }
            }
        }
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        List<LpgReader> readers = inputs.stream().map( in -> (LpgReader) in ).toList();
        LpgWriter writer = ctx.createLpgWriter( 0 );
        for ( LpgReader reader : readers ) {
            writer.writeNode( reader.getNodeIterator(), ctx );
        }
        for ( LpgReader reader : readers ) {
            writer.writeEdge( reader.getEdgeIterator(), ctx );
        }
    }

}

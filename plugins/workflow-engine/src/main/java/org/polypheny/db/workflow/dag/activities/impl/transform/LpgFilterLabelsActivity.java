/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.workflow.dag.activities.impl.transform;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.type.entity.PolyString;
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
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.DefaultGroup;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.annotations.Group.Subgroup;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.LpgInputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "lpgFilterLabels", displayName = "Filter Graph by Labels", categories = { ActivityCategory.TRANSFORM, ActivityCategory.GRAPH, ActivityCategory.CLEANING, ActivityCategory.ESSENTIALS },
        inPorts = { @InPort(type = PortType.LPG) },
        outPorts = { @OutPort(type = PortType.LPG) },
        shortDescription = "Computes a subgraph of the input that only includes nodes and edges with the specified labels."
)
@DefaultGroup(subgroups = { @Subgroup(key = "nodes", displayName = "Nodes"), @Subgroup(key = "edges", displayName = "Edges") })

@BoolSetting(key = "filterNodes", displayName = "Filter Nodes", defaultValue = true, subGroup = "nodes", pos = 0)
@FieldSelectSetting(key = "nodeLabels", displayName = "Select Node Labels", reorder = false,
        targetInput = 0, forLabels = true,
        subPointer = "filterNodes", subValues = { "true" }, subGroup = "nodes", pos = 1,
        shortDescription = "Specify the nodes to include by their label.")
@BoolSetting(key = "nodeTie", displayName = "Include Conflicting", defaultValue = false,
        subPointer = "filterNodes", subValues = { "true" }, subGroup = "nodes", pos = 2,
        shortDescription = "Whether a node that contains both label(s) to include and exclude should be included.")

@BoolSetting(key = "filterEdges", displayName = "Filter Edges", defaultValue = false, subGroup = "edges", pos = 0)
@FieldSelectSetting(key = "edgeLabels", displayName = "Select Edge Labels", reorder = false,
        targetInput = 0, forLabels = true,
        subPointer = "filterEdges", subValues = { "true" }, subGroup = "edges", pos = 1,
        shortDescription = "Specify the edges to include by their label.")
@BoolSetting(key = "edgeTie", displayName = "Include Conflicting", defaultValue = false,
        subPointer = "filterEdges", subValues = { "true" }, subGroup = "edges", pos = 2,
        shortDescription = "Whether a edge that contains both label(s) to include and exclude should be included.")

@SuppressWarnings("unused")
public class LpgFilterLabelsActivity implements Activity, Pipeable {


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        Set<String> nodes = new HashSet<>();
        Set<String> edges = new HashSet<>();
        Set<String> props = new HashSet<>();
        if ( inTypes.get( 0 ) instanceof LpgType lpg ) {
            nodes.addAll( lpg.getKnownLabels() );
            edges.addAll( lpg.getKnownProperties() );
            props.addAll( lpg.getKnownProperties() );
        }
        if ( settings.keysPresent( "filterNodes", "nodeLabels" ) && settings.getBool( "filterNodes" ) ) {
            FieldSelectValue nodeLabels = settings.getOrThrow( "nodeLabels", FieldSelectValue.class );
            nodeLabels.getExclude().forEach( nodes::remove );
            nodes.addAll( nodeLabels.getInclude() );
        }
        if ( settings.keysPresent( "filterEdges", "edgeLabels" ) && settings.getBool( "filterEdges" ) ) {
            FieldSelectValue edgeLabels = settings.getOrThrow( "edgeLabels", FieldSelectValue.class );
            edgeLabels.getExclude().forEach( edges::remove );
            edges.addAll( edgeLabels.getInclude() );
        }
        return LpgType.of( nodes, edges, props ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Pipeable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return inTypes.get( 0 );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        LpgInputPipe input = inputs.get( 0 ).asLpgInputPipe();
        boolean filterNodes = settings.getBool( "filterNodes" );
        boolean filterEdges = settings.getBool( "filterEdges" );
        FieldSelectValue nodeLabels = settings.get( "nodeLabels", FieldSelectValue.class );
        FieldSelectValue edgeLabels = settings.get( "edgeLabels", FieldSelectValue.class );
        boolean onNodeTie = settings.getBool( "nodeTie" );
        boolean onEdgeTie = settings.getBool( "edgeTie" );

        Set<PolyString> nodes = new HashSet<>();
        if ( filterNodes ) {
            for ( PolyNode node : input.getNodeIterable() ) {
                Set<String> labels = node.labels.getValue().stream().map( l -> l.value ).collect( Collectors.toSet() );
                if ( nodeLabels.isSelected( labels, onNodeTie ) ) {
                    nodes.add( node.id );
                    if ( !output.put( node ) ) {
                        input.finishIteration();
                        return;
                    }
                }
            }
        } else {
            for ( PolyNode node : input.getNodeIterable() ) {
                nodes.add( node.id );
                if ( !output.put( node ) ) {
                    input.finishIteration();
                    return;
                }
            }
        }

        for ( PolyEdge edge : input.getEdgeIterable() ) {
            if ( nodes.contains( edge.getLeft() ) && nodes.contains( edge.getRight() ) ) {
                if ( filterEdges ) {
                    Set<String> labels = edge.labels.getValue().stream().map( l -> l.value ).collect( Collectors.toSet() );
                    if ( !edgeLabels.isSelected( labels, onEdgeTie ) ) {
                        continue;
                    }
                }
                if ( !output.put( edge ) ) {
                    input.finishIteration();
                    return;
                }
            }
        }
    }

}

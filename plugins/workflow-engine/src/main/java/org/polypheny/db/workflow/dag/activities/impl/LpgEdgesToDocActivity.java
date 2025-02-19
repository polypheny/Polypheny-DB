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

import static org.polypheny.db.workflow.dag.activities.impl.LpgEdgesToDocActivity.DIR_FIELD;
import static org.polypheny.db.workflow.dag.activities.impl.LpgEdgesToDocActivity.ID_FIELD;
import static org.polypheny.db.workflow.dag.activities.impl.LpgNodesToDocActivity.LABEL_FIELD;
import static org.polypheny.db.workflow.dag.settings.GroupDef.ADVANCED_GROUP;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.AdvancedGroup;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.annotations.Group.Subgroup;
import org.polypheny.db.workflow.dag.settings.BoolValue;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.LpgInputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "lpgEdgesToDoc", displayName = "Edges to Collection", categories = { ActivityCategory.TRANSFORM, ActivityCategory.GRAPH, ActivityCategory.DOCUMENT },
        inPorts = { @InPort(type = PortType.LPG, description = "The input graph.") },
        outPorts = { @OutPort(type = PortType.DOC, description = "The output collection where each document corresponds to an edge in the graph.") },
        shortDescription = "Maps edges of a graph to documents in a collection."
)
@AdvancedGroup(subgroups = {
        @Subgroup(key = "nodes", displayName = "Incident Nodes")
})

@FieldSelectSetting(key = "labels", displayName = "Target Edges", simplified = true, targetInput = 0, pos = 0, group = ADVANCED_GROUP,
        shortDescription = "Specify the edges to map by their label(s). If no label is specified, all edges are mapped to documents.")
@BoolSetting(key = "includeLabels", displayName = "Include Labels", defaultValue = true, pos = 1,
        shortDescription = "Whether to insert an array field '" + LABEL_FIELD + "' that contains the edge labels.")
@BoolSetting(key = "includeId", displayName = "Include Edge ID", pos = 2, group = ADVANCED_GROUP,
        shortDescription = "Whether to insert a '" + ID_FIELD + "' field.")
@BoolSetting(key = "includeDirection", displayName = "Include Direction", pos = 3,
        shortDescription = "Whether to insert a '" + DIR_FIELD + "' field indicating the edge direction.")

@BoolSetting(key = "includeNodes", displayName = "Include Node Data", pos = 4,
        group = ADVANCED_GROUP, subGroup = "nodes",
        shortDescription = "Whether to include the actual data of the incident nodes instead of their IDs.")
@BoolSetting(key = "includeNodeLabels", displayName = "Include Node Labels", defaultValue = true, pos = 5,
        group = ADVANCED_GROUP, subGroup = "nodes", subPointer = "includeNodes", subValues = { "true" },
        shortDescription = "Whether to insert an array field '" + LABEL_FIELD + "' in the node data that contains the node labels.")
@BoolSetting(key = "includeNodeId", displayName = "Include Node ID", pos = 6,
        group = ADVANCED_GROUP, subGroup = "nodes", subPointer = "includeNodes", subValues = { "true" },
        shortDescription = "Whether to insert a '" + LpgNodesToDocActivity.ID_FIELD + "' field in the node data.")
@SuppressWarnings("unused")
public class LpgEdgesToDocActivity implements Activity, Pipeable {

    static final String ID_FIELD = "edge_id";
    static final String FROM_FIELD = "left";
    static final String TO_FIELD = "right";
    static final String DIR_FIELD = "direction";


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        Set<String> fields = new HashSet<>( Set.of( FROM_FIELD, TO_FIELD ) );
        settings.get( "includeLabels", BoolValue.class ).ifPresent( b -> {
            if ( b.getValue() ) {
                fields.add( LABEL_FIELD );
            }
        } );
        settings.get( "includeId", BoolValue.class ).ifPresent( b -> {
            if ( b.getValue() ) {
                fields.add( ID_FIELD );
            }
        } );
        settings.get( "includeDirection", BoolValue.class ).ifPresent( b -> {
            if ( b.getValue() ) {
                fields.add( DIR_FIELD );
            }
        } );
        return DocType.of( fields ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Pipeable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getDocType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        LpgInputPipe input = inputs.get( 0 ).asLpgInputPipe();
        Set<String> labels = new HashSet<>( settings.get( "labels", FieldSelectValue.class ).getInclude() );
        boolean includeLabels = settings.getBool( "includeLabels" );
        boolean includeId = settings.getBool( "includeId" );
        boolean includeDirection = settings.getBool( "includeDirection" );

        boolean includeNodes = settings.getBool( "includeNodes" );
        boolean includeNodeLabels = settings.getBool( "includeNodeLabels" );
        boolean includeNodeId = settings.getBool( "includeNodeId" );

        Map<PolyString, PolyDocument> nodes = new HashMap<>();
        if ( includeNodes ) {
            for ( PolyNode node : input.getNodeIterable() ) {
                nodes.put( node.id, LpgNodesToDocActivity.nodeToDoc( node, includeNodeLabels, includeNodeId ) );
            }
        } else {
            input.skipNodes();
        }

        for ( PolyEdge edge : input.getEdgeIterable() ) {
            if ( !ActivityUtils.matchesLabelList( edge, labels ) ) {
                continue;
            }

            PolyDocument doc = PolyDocument.ofDocument( edge.properties );
            if ( includeNodes ) {
                doc.put( PolyString.of( FROM_FIELD ), nodes.get( edge.getLeft() ) );
                doc.put( PolyString.of( TO_FIELD ), nodes.get( edge.getRight() ) );
            } else {
                doc.put( PolyString.of( FROM_FIELD ), edge.getLeft() );
                doc.put( PolyString.of( TO_FIELD ), edge.getRight() );
            }
            if ( includeLabels ) {
                if ( edge.getLabels().size() == 1 ) {
                    doc.put( PolyString.of( LABEL_FIELD ), edge.getLabels().get( 0 ) );
                } else {
                    doc.put( PolyString.of( LABEL_FIELD ), edge.getLabels() );
                }
            }
            if ( includeId ) {
                doc.put( PolyString.of( ID_FIELD ), edge.getId() );
            }
            if ( includeDirection ) {
                doc.put( PolyString.of( DIR_FIELD ), PolyString.of( edge.direction.toString() ) );
            }
            ActivityUtils.addDocId( doc );

            if ( !output.put( doc ) ) {
                break;
            }
        }
        input.finishIteration();
    }

}

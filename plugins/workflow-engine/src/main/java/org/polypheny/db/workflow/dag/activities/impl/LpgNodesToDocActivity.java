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

import static org.polypheny.db.workflow.dag.activities.impl.LpgNodesToDocActivity.ID_FIELD;
import static org.polypheny.db.workflow.dag.activities.impl.LpgNodesToDocActivity.LABEL_FIELD;
import static org.polypheny.db.workflow.dag.settings.GroupDef.ADVANCED_GROUP;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.document.PolyDocument;
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
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
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

@ActivityDefinition(type = "lpgNodesToDoc", displayName = "Nodes to Collection", categories = { ActivityCategory.TRANSFORM, ActivityCategory.GRAPH, ActivityCategory.DOCUMENT, ActivityCategory.CROSS_MODEL },
        inPorts = { @InPort(type = PortType.LPG, description = "The input graph.") },
        outPorts = { @OutPort(type = PortType.DOC, description = "The output collection where each document corresponds to a node in the graph.") },
        shortDescription = "Maps nodes of a graph to documents in a collection."
)
@FieldSelectSetting(key = "labels", displayName = "Target Nodes", simplified = true, targetInput = 0, pos = 0,
        group = ADVANCED_GROUP,
        shortDescription = "Specify the nodes to map by their label(s). If no label is specified, all nodes are mapped to documents.")
@BoolSetting(key = "includeLabels", displayName = "Include Labels", defaultValue = true, pos = 1,
        shortDescription = "Whether to insert an array field '" + LABEL_FIELD + "' that contains the node labels.")
@BoolSetting(key = "includeId", displayName = "Include Node ID", pos = 2,
        shortDescription = "Whether to insert a '" + ID_FIELD + "' field.")
@SuppressWarnings("unused")
public class LpgNodesToDocActivity implements Activity, Pipeable {

    static final String LABEL_FIELD = "labels";
    static final String ID_FIELD = "node_id";


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        Set<String> fields = new HashSet<>();
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

        for ( PolyNode node : input.getNodeIterable() ) {
            if ( !ActivityUtils.matchesLabelList( node, labels ) ) {
                continue;
            }

            if ( !output.put( nodeToDoc( node, includeLabels, includeId ) ) ) {
                break;
            }
        }
        input.finishIteration();
    }


    public static PolyDocument nodeToDoc( PolyNode node, boolean includeLabels, boolean includeId ) {
        PolyDocument doc = PolyDocument.ofDocument( node.properties );
        if ( includeLabels ) {
            if ( node.getLabels().size() == 1 ) {
                doc.put( PolyString.of( LABEL_FIELD ), node.getLabels().get( 0 ) );
            } else {
                doc.put( PolyString.of( LABEL_FIELD ), node.getLabels() );
            }
        }
        if ( includeId ) {
            doc.put( PolyString.of( ID_FIELD ), node.getId() );
        }
        ActivityUtils.addDocId( doc );
        return doc;
    }

}

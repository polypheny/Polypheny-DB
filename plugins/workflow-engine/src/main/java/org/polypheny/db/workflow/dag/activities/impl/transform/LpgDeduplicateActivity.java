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
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.GroupDef;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.LpgInputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@Slf4j
@ActivityDefinition(type = "lpgDeduplicate", displayName = "Remove Duplicate Nodes / Edges", categories = { ActivityCategory.TRANSFORM, ActivityCategory.GRAPH, ActivityCategory.CLEANING },
        inPorts = { @InPort(type = PortType.LPG, description = "The input graph that possibly contains duplicate elements") },
        outPorts = { @OutPort(type = PortType.LPG, description = "The graph with duplicates removed") },
        shortDescription = "Removes graph elements with duplicate values in specified properties (labels are not considered). "
                + "Only one node / edge is kept for each unique combination of values in the detection properties."
)
@FieldSelectSetting(key = "labels", displayName = "Target Labels", simplified = true, pos = 0,
        targetInput = 0, forLabels = true,
        shortDescription = "Specify the target nodes or edges by their label(s). If no label is specified, all become targets.")
@FieldSelectSetting(key = "fields", displayName = "Duplicate Detection Properties", simplified = true, pos = 1,
        targetInput = 0, forLabels = false,
        shortDescription = "The properties used to determine whether a graph element is a duplicate. If left empty, the entire element (except for its ID) is used.")
@BoolSetting(key = "nodes", displayName = "Deduplicate Nodes", defaultValue = true, pos = 2, group = GroupDef.ADVANCED_GROUP)
@BoolSetting(key = "edges", displayName = "Deduplicate Edges", defaultValue = true, pos = 3, group = GroupDef.ADVANCED_GROUP)
@BoolSetting(key = "useNull", displayName = "Treat Missing Properties as Null", pos = 4, group = GroupDef.ADVANCED_GROUP,
        shortDescription = "By default, a missing detection property results in the activity to fail. If true, missing properties are instead treated like Null values.")

@SuppressWarnings("unused")
public class LpgDeduplicateActivity implements Activity, Pipeable {


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return inTypes.get( 0 ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Pipeable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getGraphType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        LpgInputPipe input = inputs.get( 0 ).asLpgInputPipe();
        List<PolyString> fields = settings.get( "fields", FieldSelectValue.class ).getInclude().stream().map( PolyString::of ).toList();
        Set<String> labels = new HashSet<>( settings.get( "labels", FieldSelectValue.class ).getInclude() );
        boolean isNodes = settings.getBool( "nodes" );
        boolean isEdges = settings.getBool( "edges" );
        boolean useNull = settings.getBool( "useNull" );

        Set<PolyDictionary> uniqueValues = new HashSet<>();
        Set<PolyString> nodeIds = new HashSet<>();
        for ( PolyNode node : input.getNodeIterable() ) {
            if ( isNodes && ActivityUtils.matchesLabelList( node, labels ) ) {
                if ( isDuplicate( uniqueValues, fields, node.properties, useNull ) ) {
                    continue;
                }
            }
            nodeIds.add( node.id );
            if ( !output.put( node ) ) {
                finish( inputs );
                return;
            }
        }

        uniqueValues.clear();
        for ( PolyEdge edge : input.getEdgeIterable() ) {
            if ( !nodeIds.contains( edge.left ) || !nodeIds.contains( edge.right ) ) {
                continue; // node was removed
            }
            if ( isEdges && ActivityUtils.matchesLabelList( edge, labels ) ) {
                if ( isDuplicate( uniqueValues, fields, edge.properties, useNull ) ) {
                    continue;
                }
            }
            if ( !output.put( edge ) ) {
                finish( inputs );
                return;
            }
        }
    }


    private boolean isDuplicate( Set<PolyDictionary> uniqueValues, List<PolyString> fields, PolyDictionary properties, boolean useNull ) {
        PolyDictionary detector;
        if ( fields.isEmpty() ) {
            detector = properties;
        } else {
            detector = new PolyDictionary();
            for ( PolyString field : fields ) {
                PolyValue value = properties.get( field );
                if ( value == null ) {
                    if ( useNull ) {
                        value = PolyNull.NULL;
                    } else {
                        throw new GenericRuntimeException( "Field " + field.value + " does not exist in element properties " + properties );
                    }
                }
                detector.put( field, value );
            }
        }
        if ( uniqueValues.contains( detector ) ) {
            return true;
        }
        uniqueValues.add( detector );
        return false;
    }

}

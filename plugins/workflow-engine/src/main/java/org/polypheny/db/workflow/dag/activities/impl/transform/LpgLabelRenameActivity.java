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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.LpgType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.FieldRenameSetting;
import org.polypheny.db.workflow.dag.settings.FieldRenameValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.LpgInputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;

@ActivityDefinition(type = "lpgLabelRename", displayName = "Rename Graph Labels", categories = { ActivityCategory.TRANSFORM, ActivityCategory.GRAPH, ActivityCategory.CLEANING, ActivityCategory.ESSENTIALS },
        inPorts = { @InPort(type = PortType.LPG, description = "The input graph") },
        outPorts = { @OutPort(type = PortType.LPG, description = "The graph with renamed labels") },
        shortDescription = "Rename the labels of a graph by defining rules."
)
@BoolSetting(key = "nodes", displayName = "Rename Node Labels", defaultValue = true, pos = 0)
@BoolSetting(key = "edges", displayName = "Rename Edge Labels", defaultValue = true, pos = 1)
@FieldRenameSetting(key = "rename", displayName = "Renaming Rules", allowRegex = true, allowIndex = false, pos = 2,
        forLabels = true,
        shortDescription = "The source labels can be selected by their actual (exact) name or with Regex. " // TODO update description
                + "The replacement can reference capture groups such as '$0' for the original label name.",
        longDescription = """
                The source labels can be selected by their actual (exact) name or by using a regular expression.
                Regex mode can be used to specify capturing groups using parentheses.
                
                In any mode, the replacement can reference a capture group (`$0`, `$1`...). For instance, the replacement `abc$0` adds the prefix `abc` to a label.
                
                Regular expressions are given in the [Java Regex dialect](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html).
                """)

@SuppressWarnings("unused")
public class LpgLabelRenameActivity implements Activity, Pipeable {

    private final Map<String, String> renameCache = new HashMap<>();
    private FieldRenameValue renamer;


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( inTypes.get( 0 ) instanceof LpgType lpgType && settings.keysPresent( "rename", "nodes", "edges" ) ) {
            boolean isNodes = settings.getBool( "nodes" );
            boolean isEdges = settings.getBool( "edges" );
            if ( !isNodes && !isEdges ) {
                throw new InvalidSettingException( "Either nodes or edges must be enabled", "edges" );
            }
            FieldRenameValue rename = settings.getOrThrow( "rename", FieldRenameValue.class );

            Set<String> nodeLabels = new HashSet<>( lpgType.getKnownLabels() );
            Set<String> edgeLabels = new HashSet<>( lpgType.getKnownProperties() );

            if ( isNodes ) {
                nodeLabels = rename.getRenamedSet( nodeLabels );
            }
            if ( isEdges ) {
                edgeLabels = rename.getRenamedSet( edgeLabels );
            }

            return LpgType.of( nodeLabels, edgeLabels, lpgType.getKnownProperties() ).asOutTypes();
        }
        return LpgType.of().asOutTypes();
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getGraphType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        LpgInputPipe input = inputs.get( 0 ).asLpgInputPipe();
        boolean isNodes = settings.getBool( "nodes" );
        boolean isEdges = settings.getBool( "edges" );
        renamer = settings.get( "rename", FieldRenameValue.class );

        for ( PolyNode node : input.getNodeIterable() ) {
            if ( isNodes ) {
                List<PolyString> labels = getRenamedLabels( node.getLabels() );
                node = new PolyNode( node.id, node.properties, labels, null );
            }

            if ( !output.put( node ) ) {
                finish( inputs );
                return;
            }
        }

        for ( PolyEdge edge : input.getEdgeIterable() ) {
            if ( isEdges ) {
                List<PolyString> labels = getRenamedLabels( edge.getLabels() );
                edge = new PolyEdge( edge.id, edge.properties, labels, edge.left, edge.right, edge.direction, null );
            }

            if ( !output.put( edge ) ) {
                finish( inputs );
                return;
            }
        }
    }


    private List<PolyString> getRenamedLabels( PolyList<PolyString> labels ) throws IllegalArgumentException {
        Set<PolyString> renamedLabels = new HashSet<>(); // set to remove duplicate labels
        for ( PolyString label : labels ) {
            String name = label.value;
            String renamed = renameCache.computeIfAbsent( name, k -> {
                String r = renamer.rename( k );
                if ( r != null ) {
                    if ( ActivityUtils.isInvalidFieldName( r ) ) {
                        throw new IllegalArgumentException( "Invalid field name: " + r );
                    }
                    return r;
                }
                return name;
            } );
            renamedLabels.add( PolyString.of( renamed ) );
        }
        return new ArrayList<>( renamedLabels );
    }


    @Override
    public void reset() {
        renameCache.clear();
        renamer = null;
    }

}

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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.polypheny.db.algebra.type.AlgDataType;
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
import org.polypheny.db.workflow.dag.activities.TypePreview.LpgType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.FieldRenameSetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.settings.FieldRenameValue;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.LpgInputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;

@ActivityDefinition(type = "lpgPropertyRename", displayName = "Rename Graph Properties", categories = { ActivityCategory.TRANSFORM, ActivityCategory.GRAPH },
        inPorts = { @InPort(type = PortType.LPG, description = "The input graph.") },
        outPorts = { @OutPort(type = PortType.LPG, description = "The graph with renamed property fields.") },
        shortDescription = "Rename the fields of node or edge properties by defining rules."
)
@FieldSelectSetting(key = "labels", displayName = "Targets", simplified = true, targetInput = 0, pos = 0,
        shortDescription = "Specify the target nodes or edges by their label(s). If no label is specified, all become targets.")
@BoolSetting(key = "nodes", displayName = "Rename Node Properties", defaultValue = true, pos = 1)
@BoolSetting(key = "edges", displayName = "Rename Edge Properties", defaultValue = true, pos = 2)

@FieldRenameSetting(key = "rename", displayName = "Renaming Rules", allowRegex = true, allowIndex = false, pos = 3,
        shortDescription = "The source fields can be selected by their actual (exact) name or with Regex. "
                + "The replacement can reference capture groups such as '$0' for the original name.",
        longDescription = """
                The source fields can be selected by their actual (exact) name or by using a regular expression.
                Regex mode can be used to specify capturing groups using parentheses.
                
                In any mode, the replacement can reference a capture group (`$0`, `$1`...). For instance, the replacement `abc$0` adds the prefix `abc` to a field name.
                
                Regular expressions are given in the [Java Regex dialect](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html).
                """)

@SuppressWarnings("unused")
public class LpgPropertyRenameActivity implements Activity, Pipeable {

    private final Map<String, String> renameCache = new HashMap<>();
    private FieldRenameValue renamer;


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( inTypes.get( 0 ) instanceof LpgType lpgType ) {
            return lpgType.asOutTypes();
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
        Set<String> labels = new HashSet<>( settings.get( "labels", FieldSelectValue.class ).getInclude() );
        boolean isNodes = settings.getBool( "nodes" );
        boolean isEdges = settings.getBool( "edges" );
        renamer = settings.get( "rename", FieldRenameValue.class );

        for ( PolyNode node : input.getNodeIterable() ) {
            if ( isNodes && (labels.isEmpty() || node.getLabels().stream().anyMatch( l -> labels.contains( l.value ) )) ) {
                PolyDictionary renamed = getRenamedProperties( node.properties );
                node = new PolyNode( node.id, renamed, node.getLabels(), null );
            }

            if ( !output.put( node ) ) {
                finish( inputs );
                return;
            }
        }

        for ( PolyEdge edge : input.getEdgeIterable() ) {
            if ( isEdges && (labels.isEmpty() || edge.getLabels().stream().anyMatch( l -> labels.contains( l.value ) )) ) {
                PolyDictionary renamed = getRenamedProperties( edge.properties );
                edge = new PolyEdge( edge.id, renamed, edge.labels, edge.source, edge.target, edge.direction, null );
            }

            if ( !output.put( edge ) ) {
                finish( inputs );
                return;
            }
        }
    }


    private PolyDictionary getRenamedProperties( PolyDictionary dict ) {
        // PolyDictionary does not have nested maps
        Map<PolyString, PolyValue> map = new HashMap<>();
        for ( Entry<PolyString, PolyValue> entry : dict.entrySet() ) {
            String renamed = getRenamed( entry.getKey().value );
            map.put( PolyString.of( renamed ), entry.getValue() );
        }
        return PolyDictionary.ofDict( map );
    }


    private String getRenamed( String name ) throws IllegalArgumentException {
        return renameCache.computeIfAbsent( name, k -> {
            String r = renamer.rename( k );
            if ( r != null ) {
                if ( ActivityUtils.isInvalidFieldName( r ) ) {
                    throw new IllegalArgumentException( "Invalid field name: " + r );
                }
                return r;
            }
            return name;
        } );
    }


    @Override
    public void reset() {
        renameCache.clear();
        renamer = null;
    }

}

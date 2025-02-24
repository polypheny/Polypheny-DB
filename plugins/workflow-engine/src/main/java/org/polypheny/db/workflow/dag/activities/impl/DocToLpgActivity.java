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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
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
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringValue;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "docToLpg", displayName = "Collection to Graph", categories = { ActivityCategory.TRANSFORM, ActivityCategory.DOCUMENT, ActivityCategory.GRAPH },
        inPorts = { @InPort(type = PortType.DOC, description = "The input collection of documents.") },
        outPorts = { @OutPort(type = PortType.LPG, description = "The output graph whose nodes reflect the structure of the input collection.") },
        shortDescription = "Maps documents to a graph. Nested documents and array entries become their own nodes."
)
@StringSetting(key = "rootLabels", displayName = "Root Node Labels", pos = 1,
        defaultValue = "Document", maxLength = 1024,
        shortDescription = "An optional list of node labels separated by comma (',') for each root node.")
@StringSetting(key = "childLabels", displayName = "Child Node Labels", pos = 2,
        defaultValue = "", maxLength = 1024,
        shortDescription = "An optional list of node labels separated by comma (',') for each child node.")
@BoolSetting(key = "simplifyArrays", displayName = "Map Array to Property", pos = 3,
        shortDescription = "Whether arrays that only contain atomic elements should be mapped to a property in a node instead of one node per element.")
@SuppressWarnings("unused")
public class DocToLpgActivity implements Activity, Pipeable {


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return LpgType.of().asOutTypes();
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
        boolean simplifyArrays = settings.getBool( "simplifyArrays" );
        List<PolyString> rootLabels = settings.get( "rootLabels", StringValue.class ).splitAndTrim( "," )
                .stream().map( PolyString::of ).toList();
        List<PolyString> childLabels = settings.get( "childLabels", StringValue.class ).splitAndTrim( "," )
                .stream().map( PolyString::of ).toList();
        List<PolyEdge> edges = new ArrayList<>();
        for ( List<PolyValue> value : inputs.get( 0 ) ) {
            PolyDocument doc = value.get( 0 ).asDocument();
            List<PolyNode> nodes = new ArrayList<>();
            PolyNode root = recursiveMap( doc, simplifyArrays, nodes, edges, childLabels );
            root.labels.clear(); // remove childLabels
            root.labels.addAll( rootLabels );

            for ( PolyNode node : nodes ) {
                if ( !output.put( node ) ) {
                    finish( inputs );
                    return;
                }
            }
        }
        for ( PolyEdge edge : edges ) {
            if ( !output.put( edge ) ) {
                finish( inputs );
                return;
            }
        }
    }


    private PolyNode recursiveMap( PolyValue value, boolean simplifyArrays, List<PolyNode> nodes, List<PolyEdge> edges, List<PolyString> nodeLabels ) {
        PolyNode node;
        if ( value instanceof PolyDocument doc ) {
            PolyDictionary dict = new PolyDictionary();
            Map<PolyString, PolyNode> subNodes = new HashMap<>();
            for ( Entry<PolyString, PolyValue> entry : doc.entrySet() ) {
                PolyString key = entry.getKey();
                PolyValue subValue = entry.getValue();
                if ( ActivityUtils.isAtomicValue( subValue ) ) {
                    dict.put( key, subValue );
                } else {
                    subNodes.put( key, recursiveMap( subValue, simplifyArrays, nodes, edges, nodeLabels ) );
                }
            }
            node = new PolyNode( dict, nodeLabels, null );

            for ( Entry<PolyString, PolyNode> subNode : subNodes.entrySet() ) {
                PolyEdge edge = new PolyEdge( new PolyDictionary(), List.of( subNode.getKey() ), node.id, subNode.getValue().id, EdgeDirection.LEFT_TO_RIGHT, null );
                edges.add( edge );
            }
        } else if ( value instanceof PolyList<?> list ) {
            PolyDictionary dict = new PolyDictionary();
            if ( simplifyArrays && list.stream().allMatch( ActivityUtils::isAtomicValue ) ) {
                dict.put( PolyString.of( "elements" ), list );
                node = new PolyNode( dict, nodeLabels, null );
            } else {
                node = new PolyNode( dict, nodeLabels, null );
                Map<PolyString, PolyNode> subNodes = new HashMap<>();
                for ( int i = 0; i < list.size(); i++ ) {
                    PolyValue entry = list.get( i );
                    PolyNode subNode = recursiveMap( entry, simplifyArrays, nodes, edges, nodeLabels );
                    PolyEdge edge = new PolyEdge( new PolyDictionary(), List.of( PolyString.of( String.valueOf( i ) ) ), node.id, subNode.id, EdgeDirection.LEFT_TO_RIGHT, null );
                    edges.add( edge );
                }
            }
        } else {
            PolyDictionary dict = new PolyDictionary();
            dict.put( PolyString.of( "value" ), value );
            node = new PolyNode( dict, nodeLabels, null );
        }
        node.properties.entrySet().removeIf( e -> e == null || e.getValue().isNull() ); // TODO: ensure this is the best way to handle null (not supported by graph model)
        nodes.add( node );
        return node;
    }

}

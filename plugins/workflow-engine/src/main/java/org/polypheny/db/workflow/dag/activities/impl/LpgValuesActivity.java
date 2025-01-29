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

import static org.polypheny.db.workflow.dag.activities.impl.RelValuesActivity.LAST_NAMES;
import static org.polypheny.db.workflow.dag.activities.impl.RelValuesActivity.NAMES;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.LpgType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.DefaultGroup;
import org.polypheny.db.workflow.dag.annotations.DoubleSetting;
import org.polypheny.db.workflow.dag.annotations.Group.Subgroup;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.BoolValue;
import org.polypheny.db.workflow.dag.settings.IntValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringValue;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.LpgWriter;

@Slf4j

@ActivityDefinition(type = "lpgValues", displayName = "Generate Graph", categories = { ActivityCategory.EXTRACT, ActivityCategory.GRAPH },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.LPG) }
)
@DefaultGroup(subgroups = { @Subgroup(key = "nodes", displayName = "Nodes"), @Subgroup(key = "edges", displayName = "Edges") })
@BoolSetting(key = "fixSeed", displayName = "Fix Random Seed", defaultValue = false)

@IntSetting(key = "count", displayName = "Node Count", defaultValue = 3, min = 1, max = 1_000, subGroup = "nodes", pos = 0)
@StringSetting(key = "nodeLabels", displayName = "Node Labels", shortDescription = "A list of node labels separated by comma (',') to sample from.",
        defaultValue = "Person", nonBlank = true, subGroup = "nodes", pos = 1)
@IntSetting(key = "nodeLabelCount", displayName = "Node Label Count", shortDescription = "The number of labels per node.",
        defaultValue = 1, min = 1, max = 10, subGroup = "nodes", pos = 2)

@DoubleSetting(key = "edgeDensity", displayName = "Edges per Node", defaultValue = 0.5, min = 0, max = 1, subGroup = "edges", pos = 0)
@StringSetting(key = "edgeLabels", displayName = "Edge Labels", shortDescription = "A list of edge labels separated by comma (',') to sample from.",
        defaultValue = "KNOWS", nonBlank = true, subGroup = "edges", pos = 1)
@IntSetting(key = "edgeLabelCount", displayName = "Edge Label Count", shortDescription = "The number of labels per edge.",
        defaultValue = 1, min = 1, max = 10, subGroup = "edges", pos = 2)

@SuppressWarnings("unused")
public class LpgValuesActivity implements Activity, Pipeable {


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( settings.keysPresent( "nodeLabels", "nodeLabelCount" ) &&
                settings.getOrThrow( "nodeLabels", StringValue.class ).splitAndTrim( "," ).size() < settings.getInt( "nodeLabelCount" ) ) {
            throw new InvalidSettingException( "The node label count must not be larger than the number of specified labels.", "nodeLabelCount" );
        }
        if ( settings.keysPresent( "edgeLabels", "edgeLabelCount" ) &&
                settings.getOrThrow( "edgeLabels", StringValue.class ).splitAndTrim( "," ).size() < settings.getInt( "edgeLabelCount" ) ) {
            throw new InvalidSettingException( "The edge label count must not be larger than the number of specified labels.", "edgeLabelCount" );
        }

        Set<String> nodeLabels = settings.get( "nodeLabels", StringValue.class )
                .map( s -> (Set<String>) new HashSet<>( s.splitAndTrim( "," ) ) )
                .orElse( Set.of() );
        Set<String> edgeLabels = settings.get( "edgeLabels", StringValue.class )
                .map( s -> (Set<String>) new HashSet<>( s.splitAndTrim( "," ) ) )
                .orElse( Set.of() );
        return LpgType.of( nodeLabels, edgeLabels ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        boolean fixSeed = settings.get( "fixSeed", BoolValue.class ).getValue();

        List<String> nodeLabels = settings.get( "nodeLabels", StringValue.class ).splitAndTrim( "," );
        List<String> edgeLabels = settings.get( "edgeLabels", StringValue.class ).splitAndTrim( "," );
        int nodeLabelCount = settings.getInt( "nodeLabelCount" );
        int edgeLabelCount = settings.getInt( "nodeLabelCount" );

        List<PolyNode> nodes = getNodes( settings.getInt( "count" ), fixSeed, nodeLabels, nodeLabelCount );
        List<PolyEdge> edges = getEdges( nodes.stream().map( PolyNode::getId ).toList(),
                settings.getDouble( "edgeDensity" ), fixSeed, edgeLabels, edgeLabelCount );

        LpgWriter writer = ctx.createLpgWriter( 0 );
        writer.writeNode( nodes.iterator(), ctx );
        writer.writeEdge( edges.iterator(), ctx );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getGraphType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        int n = settings.getInt( "count" );
        boolean fixSeed = settings.getBool( "fixSeed" );
        double edgeDensity = settings.getDouble( "edgeDensity" );

        List<String> nodeLabels = settings.get( "nodeLabels", StringValue.class ).splitAndTrim( "," );
        List<String> edgeLabels = settings.get( "edgeLabels", StringValue.class ).splitAndTrim( "," );
        int nodeLabelCount = settings.getInt( "nodeLabelCount" );
        int edgeLabelCount = settings.getInt( "nodeLabelCount" );

        List<String> shuffled = new ArrayList<>( nodeLabels );
        Random random = fixSeed ? new Random( 42 ) : new Random();
        List<PolyString> nodeIds = new ArrayList<>();
        for ( int i = 0; i < n; i++ ) {
            PolyNode node = getNode( random, shuffled, nodeLabelCount );
            nodeIds.add( node.id );
            if ( !output.put( node ) ) {
                return;
            }
        }

        shuffled = new ArrayList<>( edgeLabels );
        if ( edgeDensity > 0 && n > 1 ) {
            random = fixSeed ? new Random( 42 ) : random;
            for ( int i = 0; i < n; i++ ) {
                if ( random.nextDouble() < edgeDensity ) {
                    int target;
                    do {
                        target = random.nextInt( n );
                    } while ( target == i );

                    Collections.shuffle( shuffled, random );
                    if ( !output.put( getEdge( nodeIds, i, target, shuffled.subList( 0, edgeLabelCount ) ) ) ) {
                        return;
                    }
                }
            }
        }
    }


    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        return settings.get( "count", IntValue.class ).getValue();
    }


    private static List<PolyNode> getNodes( int n, boolean fixSeed, List<String> labels, int labelCount ) {
        Random random = fixSeed ? new Random( 42 ) : new Random();
        List<PolyNode> nodes = new ArrayList<>();
        List<String> shuffled = new ArrayList<>( labels ); // make editable
        for ( int i = 0; i < n; i++ ) {
            nodes.add( getNode( random, shuffled, labelCount ) );
        }
        return nodes;
    }


    private static List<PolyEdge> getEdges( List<PolyString> nodeIds, double edgeDensity, boolean fixSeed, List<String> labels, int labelCount ) {
        if ( edgeDensity == 0 || nodeIds.size() < 2 ) {
            return List.of();
        }
        List<String> shuffled = new ArrayList<>( labels );
        Random random = fixSeed ? new Random( 42 ) : new Random();
        List<PolyEdge> edges = new ArrayList<>();
        int n = nodeIds.size();
        for ( int i = 0; i < n; i++ ) {
            if ( random.nextDouble() < edgeDensity ) {
                int target;
                do {
                    target = random.nextInt( n );
                } while ( target == i );
                Collections.shuffle( shuffled, random );
                edges.add( getEdge( nodeIds, i, target, shuffled.subList( 0, labelCount ) ) );
            }
        }
        return edges;
    }


    private static PolyNode getNode( Random random, List<String> shuffled, int labelCount ) {
        String firstName = NAMES.get( random.nextInt( NAMES.size() ) );
        String lastName = LAST_NAMES.get( random.nextInt( LAST_NAMES.size() ) );
        int age = random.nextInt( 18, 66 );
        int salary = random.nextInt( 5000, 10000 );

        Collections.shuffle( shuffled, random );
        return getNode( firstName, lastName, age, salary, shuffled.subList( 0, labelCount ) );
    }


    private static PolyNode getNode( String name, String lastName, int age, int salary, List<String> labels ) {
        Map<PolyString, PolyValue> map = new HashMap<>();
        map.put( PolyString.of( "name" ), PolyString.of( name ) );
        map.put( PolyString.of( "lastName" ), PolyString.of( lastName ) );
        map.put( PolyString.of( "age" ), PolyInteger.of( age ) );
        map.put( PolyString.of( "salary" ), PolyInteger.of( salary ) );
        return new PolyNode( new PolyDictionary( map ), labels.stream().map( PolyString::of ).toList(), null );
    }


    private static PolyEdge getEdge( List<PolyString> nodes, int fromIdx, int toIdx, List<String> labels ) {
        return new PolyEdge( new PolyDictionary(), labels.stream().map( PolyString::of ).toList(),
                nodes.get( fromIdx ), nodes.get( toIdx ), EdgeDirection.LEFT_TO_RIGHT, null );
    }

}

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.GraphType;
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
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.DoubleSetting;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.settings.BoolValue;
import org.polypheny.db.workflow.dag.settings.DoubleValue;
import org.polypheny.db.workflow.dag.settings.IntValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
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
@IntSetting(key = "count", displayName = "Node Count", defaultValue = 3, min = 1, max = 1_000)
@DoubleSetting(key = "edgeDensity", displayName = "Edges per Node", defaultValue = 0.5, min = 0, max = 1)
@BoolSetting(key = "fixSeed", displayName = "Fix Random Seed", defaultValue = false)

@SuppressWarnings("unused")
public class LpgValuesActivity implements Activity, Pipeable {

    private static final String NODE_LABEL = "Person";
    private static final String EDGE_LABEL = "KNOWS";


    @Override
    public List<Optional<AlgDataType>> previewOutTypes( List<Optional<AlgDataType>> inTypes, SettingsPreview settings ) throws ActivityException {
        return Activity.wrapType( getType() );
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        boolean fixSeed = settings.get( "fixSeed", BoolValue.class ).getValue();
        List<PolyNode> nodes = getNodes( settings.get( "count", IntValue.class ).getValue(), fixSeed );
        List<PolyEdge> edges = getEdges( nodes.stream().map( PolyNode::getId ).toList(), settings.get( "edgeDensity", DoubleValue.class ).getValue(), fixSeed );

        LpgWriter writer = ctx.createLpgWriter( 0 );
        writer.writeNode( nodes.iterator() );
        writer.writeEdge( edges.iterator() );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        int n = settings.get( "count", IntValue.class ).getValue();
        boolean fixSeed = settings.get( "fixSeed", BoolValue.class ).getValue();
        double edgeDensity = settings.get( "edgeDensity", DoubleValue.class ).getValue();

        Random random = fixSeed ? new Random( 42 ) : new Random();
        List<PolyString> nodeIds = new ArrayList<>();
        for ( int i = 0; i < n; i++ ) {
            PolyNode node = getNode( random );
            nodeIds.add( node.id );
            output.put( node );
        }

        if ( edgeDensity > 0 && n > 1 ) {
            random = fixSeed ? new Random( 42 ) : random;
            for ( int i = 0; i < n; i++ ) {
                if ( random.nextDouble() < edgeDensity ) {
                    int target;
                    do {
                        target = random.nextInt( n );
                    } while ( target == i );
                    output.put( getEdge( nodeIds, i, target ) );
                }
            }
        }
    }


    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        return settings.get( "count", IntValue.class ).getValue();
    }


    @Override
    public void reset() {

    }


    private static AlgDataType getType() {
        return GraphType.of();
    }


    private static List<PolyNode> getNodes( int n, boolean fixSeed ) {
        Random random = fixSeed ? new Random( 42 ) : new Random();
        List<PolyNode> nodes = new ArrayList<>();
        for ( int i = 0; i < n; i++ ) {
            nodes.add( getNode( random ) );
        }
        return nodes;
    }


    private static List<PolyEdge> getEdges( List<PolyString> nodeIds, double edgeDensity, boolean fixSeed ) {
        if ( edgeDensity == 0 || nodeIds.size() < 2 ) {
            return List.of();
        }
        Random random = fixSeed ? new Random( 42 ) : new Random();
        List<PolyEdge> edges = new ArrayList<>();
        int n = nodeIds.size();
        for ( int i = 0; i < n; i++ ) {
            if ( random.nextDouble() < edgeDensity ) {
                int target;
                do {
                    target = random.nextInt( n );
                } while ( target == i );
                edges.add( getEdge( nodeIds, i, target ) );
            }
        }
        return edges;
    }


    private static PolyNode getNode( Random random ) {
        String firstName = NAMES.get( random.nextInt( NAMES.size() ) );
        String lastName = LAST_NAMES.get( random.nextInt( LAST_NAMES.size() ) );
        int age = random.nextInt( 18, 66 );
        int salary = random.nextInt( 5000, 10000 );
        return getNode( firstName, lastName, age, salary );
    }


    private static PolyNode getNode( String name, String lastName, int age, int salary ) {
        Map<PolyString, PolyValue> map = new HashMap<>();
        map.put( PolyString.of( "name" ), PolyString.of( name ) );
        map.put( PolyString.of( "lastName" ), PolyString.of( lastName ) );
        map.put( PolyString.of( "age" ), PolyInteger.of( age ) );
        map.put( PolyString.of( "salary" ), PolyInteger.of( salary ) );
        return new PolyNode( new PolyDictionary( map ), List.of( PolyString.of( NODE_LABEL ) ), null );
    }


    private static PolyEdge getEdge( List<PolyString> nodes, int fromIdx, int toIdx ) {
        return new PolyEdge( new PolyDictionary(), List.of( PolyString.of( EDGE_LABEL ) ),
                nodes.get( fromIdx ), nodes.get( toIdx ), EdgeDirection.LEFT_TO_RIGHT, null );
    }

}

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

package org.polypheny.db.workflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.graph.TopologicalOrderIterator;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.WorkflowImpl;
import org.polypheny.db.workflow.dag.activities.ActivityRegistry;
import org.polypheny.db.workflow.dag.settings.EntityValue;
import org.polypheny.db.workflow.engine.storage.StorageUtils;
import org.polypheny.db.workflow.models.ActivityModel;
import org.polypheny.db.workflow.models.EdgeModel;
import org.polypheny.db.workflow.models.WorkflowConfigModel;
import org.polypheny.db.workflow.models.WorkflowModel;

public class WorkflowUtils {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final BooleanNode TRUE = BooleanNode.TRUE;
    private static final BooleanNode FALSE = BooleanNode.FALSE;


    public static Workflow getWorkflow( List<ActivityModel> activities, List<EdgeModel> edges, boolean fusionEnabled, boolean pipelineEnabled, int maxWorkers ) {
        WorkflowConfigModel config = new WorkflowConfigModel(
                Map.of( DataModel.RELATIONAL, "hsqldb", DataModel.DOCUMENT, "hsqldb", DataModel.GRAPH, "hsqldb" ),
                fusionEnabled,
                pipelineEnabled,
                maxWorkers,
                10 // low on purpose to observe blocking
        );
        Map<String, JsonNode> variables = Map.of( "creationTime", TextNode.valueOf( LocalDateTime.now().format( DateTimeFormatter.ISO_DATE_TIME ) ) );
        return WorkflowImpl.fromModel( new WorkflowModel( activities, edges, config, variables, null ) );
    }


    public static Workflow getWorkflow1() {
        List<ActivityModel> activities = List.of(
                new ActivityModel( "relValues" ),
                new ActivityModel( "debug" )
        );
        List<EdgeModel> edges = List.of(
                EdgeModel.of( activities.get( 0 ), activities.get( 1 ) )
        );
        return getWorkflow( activities, edges, false, false, 1 );
    }


    public static Workflow getUnionWorkflow() {
        List<ActivityModel> activities = List.of(
                new ActivityModel( "relValues" ),
                new ActivityModel( "relValues" ),
                new ActivityModel( "relUnion" )
        );
        List<EdgeModel> edges = List.of(
                EdgeModel.of( activities.get( 0 ), activities.get( 2 ), 0 ),
                EdgeModel.of( activities.get( 1 ), activities.get( 2 ), 1 ),
                EdgeModel.of( activities.get( 0 ), activities.get( 1 ), true ) // ensure consistent ordering
        );
        return getWorkflow( activities, edges, false, false, 1 );
    }


    public static Workflow getMergeWorkflow( boolean simulateFailure ) {
        Map<String, JsonNode> settings = new HashMap<>( ActivityRegistry.getSerializableDefaultSettings( "debug" ) );
        settings.put( "isSuccessful", BooleanNode.valueOf( !simulateFailure ) );

        List<ActivityModel> activities = List.of(
                new ActivityModel( "relValues" ),
                new ActivityModel( "relValues" ),
                new ActivityModel( "debug", settings ),
                new ActivityModel( "relMerge" )
        );
        List<EdgeModel> edges = List.of(
                EdgeModel.of( activities.get( 0 ), activities.get( 3 ), 0 ),
                EdgeModel.of( activities.get( 1 ), activities.get( 2 ), 0 ),
                EdgeModel.of( activities.get( 2 ), activities.get( 3 ), 1 ),
                EdgeModel.of( activities.get( 0 ), activities.get( 1 ), true ) // ensure consistent ordering
        );
        return getWorkflow( activities, edges, false, false, 1 );
    }


    public static Workflow getSimpleFusion() {
        List<ActivityModel> activities = List.of(
                new ActivityModel( "relValues" ),
                new ActivityModel( "identity" )
        );
        List<EdgeModel> edges = List.of(
                EdgeModel.of( activities.get( 0 ), activities.get( 1 ), 0 )
        );
        return getWorkflow( activities, edges, true, false, 1 );
    }


    public static Pair<Workflow, List<UUID>> getAdvancedFusion() {
        // 0 - 3 - 4 should fuse
        List<ActivityModel> activities = List.of(
                new ActivityModel( "relExtract", extractTableSetting() ),
                new ActivityModel( "relExtract", extractTableSetting() ),
                new ActivityModel( "debug" ), // -> cannot fuse
                new ActivityModel( "relUnion" ),
                new ActivityModel( "identity" )
        );
        List<EdgeModel> edges = List.of(
                EdgeModel.of( activities.get( 0 ), activities.get( 3 ), 0 ),
                EdgeModel.of( activities.get( 1 ), activities.get( 2 ), 0 ),
                EdgeModel.of( activities.get( 2 ), activities.get( 3 ), 1 ),
                EdgeModel.of( activities.get( 3 ), activities.get( 4 ), 0 )
        );
        return getWorkflowWithActivities( activities, edges, true, false, 1 ); // also return ids, since topological order is not stable
    }


    public static Workflow getRelValuesFusion() {
        // 0 - 3 should fuse
        List<ActivityModel> activities = List.of(
                new ActivityModel( "relValues" ),
                new ActivityModel( "relValues" ),
                new ActivityModel( "debug" ), // -> cannot fuse
                new ActivityModel( "relUnion" )
        );
        List<EdgeModel> edges = List.of(
                EdgeModel.of( activities.get( 0 ), activities.get( 3 ), 0 ),
                EdgeModel.of( activities.get( 1 ), activities.get( 2 ), 0 ),
                EdgeModel.of( activities.get( 2 ), activities.get( 3 ), 1 )
        );
        return getWorkflow( activities, edges, true, false, 1 );
    }


    public static Workflow getExtractWorkflow() {
        List<ActivityModel> activities = List.of(
                new ActivityModel( "relExtract", extractTableSetting() ),
                new ActivityModel( "identity" )
        );
        List<EdgeModel> edges = List.of(
                EdgeModel.of( activities.get( 0 ), activities.get( 1 ), 0 )
        );
        return getWorkflow( activities, edges, true, false, 1 );
    }


    public static Workflow getSimplePipe() {
        List<ActivityModel> activities = List.of(
                new ActivityModel( "relValues", Map.of( "rowCount", IntNode.valueOf( 20 ) ) ),
                new ActivityModel( "identity" ),
                new ActivityModel( "identity" )
        );
        List<EdgeModel> edges = List.of(
                EdgeModel.of( activities.get( 0 ), activities.get( 1 ), 0 ),
                EdgeModel.of( activities.get( 1 ), activities.get( 2 ), 0 )
        );
        return getWorkflow( activities, edges, false, true, 1 );
    }


    public static Workflow getLongRunningPipe( int minMillis ) {
        int n = 100;
        List<ActivityModel> activities = List.of(
                new ActivityModel( "relValues", Map.of( "rowCount", IntNode.valueOf( n ) ) ),
                new ActivityModel( "debug", Map.of( "pipeDelay", IntNode.valueOf( minMillis / n + 1 ), "canPipe", TRUE ) ),
                new ActivityModel( "identity" )
        );
        List<EdgeModel> edges = List.of(
                EdgeModel.of( activities.get( 0 ), activities.get( 1 ), 0 ),
                EdgeModel.of( activities.get( 1 ), activities.get( 2 ), 0 )
        );
        return getWorkflow( activities, edges, false, true, 1 );
    }


    public static Pair<Workflow, List<UUID>> getCombinedFuseAndPipe() {
        // 0 - 4 - 5  should fuse
        // 3 -/

        // 1 = 2 should pipe
        List<ActivityModel> activities = List.of(
                new ActivityModel( "relExtract", extractTableSetting() ),
                new ActivityModel( "relExtract", extractTableSetting() ),
                new ActivityModel( "debug", Map.of( "canPipe", TRUE ) ), // can only pipe, not fuse!
                new ActivityModel( "identity" ),
                new ActivityModel( "relUnion" ),
                new ActivityModel( "identity" )
        );
        List<EdgeModel> edges = List.of(
                EdgeModel.of( activities.get( 0 ), activities.get( 4 ), 0 ),
                EdgeModel.of( activities.get( 1 ), activities.get( 2 ), 0 ),
                EdgeModel.of( activities.get( 2 ), activities.get( 3 ), 0 ),
                EdgeModel.of( activities.get( 3 ), activities.get( 4 ), 1 ), // a greedy optimizer would create the checkpoint here instead of 2 -> 3, which is not optimal
                EdgeModel.of( activities.get( 4 ), activities.get( 5 ), 0 )
        );
        return getWorkflowWithActivities( activities, edges, true, true, 1 );
    }


    public static Workflow getVariableWritingWorkflow() {
        List<ActivityModel> activities = List.of(
                new ActivityModel( "relValues" ),
                new ActivityModel( "fieldNameToVar" ),
                new ActivityModel( "varToRow" )
        );
        List<EdgeModel> edges = List.of(
                EdgeModel.of( activities.get( 0 ), activities.get( 1 ), 0 ),
                EdgeModel.of( activities.get( 1 ), activities.get( 2 ), true )
        );
        return getWorkflow( activities, edges, false, false, 1 );
    }

    public static Workflow getParallelBranchesWorkflow(int nBranches, int millisPerBranch, int maxWorkers) {
        assert nBranches > 0 && millisPerBranch > 0;
        ActivityModel root = new ActivityModel( "relValues" );
        List<ActivityModel> activities = new ArrayList<>(List.of(root));
        List<EdgeModel> edges = new ArrayList<>();

        for (int i = 0; i<nBranches; i++) {
            ActivityModel branch = new ActivityModel( "debug", Map.of( "delay", IntNode.valueOf( millisPerBranch ) ) );
            activities.add( branch );
            edges.add( EdgeModel.of( root, branch ) );
        }
        return getWorkflow( activities, edges, false, false, maxWorkers );
    }


    public static List<UUID> getTopologicalActivityIds( Workflow workflow ) {
        List<UUID> list = new ArrayList<>();
        for ( UUID n : TopologicalOrderIterator.of( workflow.toDag() ) ) {
            list.add( n );
        }
        return list;
    }


    public static JsonNode getEntitySetting( String namespace, String name ) {
        return new EntityValue( namespace, name ).toJson( mapper );
    }


    private static Map<String, JsonNode> extractTableSetting() {
        return Map.of( "table", getEntitySetting( "public", StorageUtils.REL_TABLE ) );
    }


    private static Pair<Workflow, List<UUID>> getWorkflowWithActivities( List<ActivityModel> activities, List<EdgeModel> edges, boolean fusionEnabled, boolean pipelineEnabled, int maxWorkers ) {
        return Pair.of(
                getWorkflow( activities, edges, fusionEnabled, pipelineEnabled, maxWorkers ),
                activities.stream().map( ActivityModel::getId ).toList() );
    }

}

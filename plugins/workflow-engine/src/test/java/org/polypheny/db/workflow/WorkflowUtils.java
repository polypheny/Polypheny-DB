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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.graph.TopologicalOrderIterator;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.WorkflowImpl;
import org.polypheny.db.workflow.dag.activities.Activity.ControlStateMerger;
import org.polypheny.db.workflow.dag.activities.impl.DocExtractActivity;
import org.polypheny.db.workflow.dag.activities.impl.LpgExtractActivity;
import org.polypheny.db.workflow.dag.settings.EntityValue;
import org.polypheny.db.workflow.engine.storage.StorageUtils;
import org.polypheny.db.workflow.models.ActivityConfigModel;
import org.polypheny.db.workflow.models.ActivityConfigModel.CommonType;
import org.polypheny.db.workflow.models.ActivityModel;
import org.polypheny.db.workflow.models.EdgeModel;
import org.polypheny.db.workflow.models.RenderModel;
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
        try {
            return WorkflowImpl.fromModel( new WorkflowModel( activities, edges, config, variables ) );
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
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
        List<ActivityModel> activities = List.of(
                new ActivityModel( "relValues" ),
                new ActivityModel( "relValues" ),
                new ActivityModel( "debug", Map.of( "isSuccessful", BooleanNode.valueOf( !simulateFailure ) ) ),
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


    public static Workflow getParallelBranchesWorkflow( int nBranches, int millisPerBranch, int maxWorkers ) {
        assert nBranches > 0 && millisPerBranch > 0;
        ActivityModel root = new ActivityModel( "relValues" );
        List<ActivityModel> activities = new ArrayList<>( List.of( root ) );
        List<EdgeModel> edges = new ArrayList<>();

        for ( int i = 0; i < nBranches; i++ ) {
            ActivityModel branch = new ActivityModel( "debug", Map.of( "delay", IntNode.valueOf( millisPerBranch ) ) );
            activities.add( branch );
            edges.add( EdgeModel.of( root, branch ) );
        }
        return getWorkflow( activities, edges, false, false, maxWorkers );
    }


    public static Pair<Workflow, List<UUID>> getCommonTransactionsWorkflow( boolean isFailingExtract ) {
        List<ActivityModel> activities = List.of(
                getCommonActivity( "relValues", Map.of(), CommonType.EXTRACT ),
                getCommonActivity( "relValues", Map.of(), CommonType.EXTRACT ),
                getCommonActivity( "debug", Map.of( "isSuccessful", BooleanNode.valueOf( !isFailingExtract ) ), CommonType.EXTRACT ),
                new ActivityModel( "identity" ),
                new ActivityModel( "identity" ),
                new ActivityModel( "relValues" ),
                getCommonActivity( "identity", Map.of(), CommonType.LOAD ),
                getCommonActivity( "relUnion", Map.of(), CommonType.LOAD )

        );
        List<EdgeModel> edges = List.of(
                EdgeModel.of( activities.get( 0 ), activities.get( 3 ) ),
                EdgeModel.of( activities.get( 1 ), activities.get( 2 ) ),
                EdgeModel.of( activities.get( 1 ), activities.get( 5 ), false ), // should be executed if extract fails
                EdgeModel.of( activities.get( 2 ), activities.get( 4 ) ),
                EdgeModel.of( activities.get( 3 ), activities.get( 6 ) ),
                EdgeModel.of( activities.get( 3 ), activities.get( 7 ), 0 ),
                EdgeModel.of( activities.get( 4 ), activities.get( 7 ), 1 )
        );
        return getWorkflowWithActivities( activities, edges, false, false, 1 );
    }


    public static Pair<Workflow, List<UUID>> getCommonExtractSkipActivityWorkflow() {
        List<ActivityModel> activities = List.of(
                getCommonActivity( "relValues", Map.of(), CommonType.EXTRACT ),
                getCommonActivity( "debug", Map.of( "isSuccessful", FALSE ), CommonType.EXTRACT ),
                getCommonActivity( "identity", Map.of(), CommonType.EXTRACT ),
                new ActivityModel( "relValues" )
        );
        List<EdgeModel> edges = List.of(
                EdgeModel.of( activities.get( 0 ), activities.get( 1 ) ),
                EdgeModel.of( activities.get( 0 ), activities.get( 3 ), false ),
                EdgeModel.of( activities.get( 1 ), activities.get( 2 ) )
        );
        return getWorkflowWithActivities( activities, edges, false, false, 1 );
    }


    public static Pair<Workflow, List<UUID>> getCommonLoadGetsSkippedWorkflow() {
        List<ActivityModel> activities = List.of(
                new ActivityModel( "relValues" ),
                new ActivityModel( "debug", Map.of( "isSuccessful", FALSE ) ),
                getCommonActivity( "identity", Map.of(), CommonType.LOAD ),
                getCommonActivity( "identity", Map.of(), CommonType.LOAD ) // gets skipped by failing debug -> should trigger load tx rollback
        );
        List<EdgeModel> edges = List.of(
                EdgeModel.of( activities.get( 0 ), activities.get( 1 ) ),
                EdgeModel.of( activities.get( 0 ), activities.get( 2 ) ),
                EdgeModel.of( activities.get( 1 ), activities.get( 3 ) )
        );
        return getWorkflowWithActivities( activities, edges, false, false, 1 );
    }


    public static Workflow getDocumentWorkflow( int nDocs ) {
        List<ActivityModel> activities = List.of(
                new ActivityModel( "docValues", Map.of( "count", IntNode.valueOf( nDocs ) ) ),
                new ActivityModel( "docIdentity" ) );
        List<EdgeModel> edges = List.of(
                EdgeModel.of( activities.get( 0 ), activities.get( 1 ) )
        );
        return getWorkflow( activities, edges, false, false, 1 );
    }


    public static Workflow getDocumentLoadAndExtract( int nDocs, String targetNamespace, String targetCollection ) {
        List<ActivityModel> activities = List.of(
                new ActivityModel( "docValues", Map.of( "count", IntNode.valueOf( nDocs ) ) ),
                new ActivityModel( "docLoad", getDocEntitySetting( targetNamespace, targetCollection ) ),
                new ActivityModel( "docExtract", getDocEntitySetting( targetNamespace, targetCollection ) ) );
        List<EdgeModel> edges = List.of(
                EdgeModel.of( activities.get( 0 ), activities.get( 1 ) ),
                EdgeModel.of( activities.get( 1 ), activities.get( 2 ), true )
        );
        return getWorkflow( activities, edges, false, false, 1 );
    }


    public static Workflow getLpgWorkflow( int nNodes, double edgeDensity, boolean pipelineEnabled ) {
        List<ActivityModel> activities = List.of(
                new ActivityModel( "lpgValues", Map.of( "count", IntNode.valueOf( nNodes ), "edgeDensity", DoubleNode.valueOf( edgeDensity ) ) ),
                new ActivityModel( "lpgIdentity" ) );
        List<EdgeModel> edges = List.of(
                EdgeModel.of( activities.get( 0 ), activities.get( 1 ) )
        );
        return getWorkflow( activities, edges, false, pipelineEnabled, 1 );
    }


    public static Workflow getLpgLoadAndExtract( int nNodes, double edgeDensity, String targetGraph ) {
        List<ActivityModel> activities = List.of(
                new ActivityModel( "lpgValues", Map.of( "count", IntNode.valueOf( nNodes ), "edgeDensity", DoubleNode.valueOf( edgeDensity ) ) ),
                new ActivityModel( "lpgLoad", getGraphEntitySetting( targetGraph ) ),
                new ActivityModel( "lpgExtract", getGraphEntitySetting( targetGraph ) ) );
        List<EdgeModel> edges = List.of(
                EdgeModel.of( activities.get( 0 ), activities.get( 1 ) ),
                EdgeModel.of( activities.get( 1 ), activities.get( 2 ), true )
        );
        return getWorkflow( activities, edges, false, false, 1 );
    }


    public static List<UUID> getTopologicalActivityIds( Workflow workflow ) {
        List<UUID> list = new ArrayList<>();
        for ( UUID n : TopologicalOrderIterator.of( workflow.toDag() ) ) {
            list.add( n );
        }
        return list;
    }


    /**
     * Used for exporting the workflows to use them in a non-test setting
     */
    public static void exportWorkflows() { // TODO: delete when no longer required
        List<Workflow> workflows = List.of(
                getUnionWorkflow(),
                getMergeWorkflow( false ),
                getSimpleFusion(),
                getAdvancedFusion().left,
                getSimplePipe(),
                getLongRunningPipe( 10000 ),
                getCombinedFuseAndPipe().left,
                getVariableWritingWorkflow(),
                getParallelBranchesWorkflow( 10, 1000, 10 ),
                getCommonTransactionsWorkflow( false ).left,
                getCommonExtractSkipActivityWorkflow().left,
                getCommonLoadGetsSkippedWorkflow().left,
                getDocumentWorkflow( 5 ),
                getLpgWorkflow( 5, 0.5, false )
        );
        for ( Workflow wf : workflows ) {
            try {
                System.out.println( mapper.writeValueAsString( wf.toModel( false ) ) );
            } catch ( JsonProcessingException e ) {
                throw new RuntimeException( e );
            }
        }
    }


    public static JsonNode getEntitySetting( String namespace, String name ) {
        return new EntityValue( namespace, name ).toJson( mapper );
    }


    private static Map<String, JsonNode> extractTableSetting() {
        return Map.of( "table", getEntitySetting( "public", StorageUtils.REL_TABLE ) );
    }


    private static Map<String, JsonNode> getDocEntitySetting( String ns, String collection ) {
        return Map.of( DocExtractActivity.COLL_KEY, getEntitySetting( ns, collection ) );
    }


    private static Map<String, JsonNode> getGraphEntitySetting( String graphName ) {
        return Map.of( LpgExtractActivity.GRAPH_KEY, getEntitySetting( graphName, graphName ) );
    }


    private static Pair<Workflow, List<UUID>> getWorkflowWithActivities( List<ActivityModel> activities, List<EdgeModel> edges, boolean fusionEnabled, boolean pipelineEnabled, int maxWorkers ) {
        return Pair.of(
                getWorkflow( activities, edges, fusionEnabled, pipelineEnabled, maxWorkers ),
                activities.stream().map( ActivityModel::getId ).toList() );
    }


    private static ActivityModel getCommonActivity( String type, Map<String, JsonNode> settings, CommonType commonType ) {
        ActivityConfigModel config = new ActivityConfigModel( false, null, commonType, ControlStateMerger.AND_AND );
        return new ActivityModel( type, UUID.randomUUID(), settings, config, RenderModel.of() );
    }

}

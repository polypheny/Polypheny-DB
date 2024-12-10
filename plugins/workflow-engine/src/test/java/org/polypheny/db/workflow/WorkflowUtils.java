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

    private static ObjectMapper mapper = new ObjectMapper();


    public static Workflow getWorkflow( List<ActivityModel> activities, List<EdgeModel> edges, boolean fusionEnabled, boolean pipelineEnabled, int maxWorkers ) {
        WorkflowConfigModel config = new WorkflowConfigModel(
                Map.of( DataModel.RELATIONAL, "hsqldb", DataModel.DOCUMENT, "hsqldb", DataModel.GRAPH, "hsqldb" ),
                fusionEnabled,
                pipelineEnabled,
                maxWorkers
        );
        return WorkflowImpl.fromModel( new WorkflowModel( activities, edges, config, null ) );
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
        Map<String, JsonNode> settings = new HashMap<>( ActivityRegistry.getSerializableSettingValues( "debug" ) );
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
                new ActivityModel( "relExtract", Map.of( "table", getEntitySetting( "public", StorageUtils.REL_TABLE ) ) ),
                new ActivityModel( "relExtract", Map.of( "table", getEntitySetting( "public", StorageUtils.REL_TABLE ) ) ),
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
        return Pair.of(
                getWorkflow( activities, edges, true, false, 1 ),
                activities.stream().map( ActivityModel::getId ).toList() ); // also return ids, since topological order is not stable
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
                new ActivityModel( "relExtract", Map.of( "table", getEntitySetting( "public", StorageUtils.REL_TABLE ) ) ),
                new ActivityModel( "identity" )
        );
        List<EdgeModel> edges = List.of(
                EdgeModel.of( activities.get( 0 ), activities.get( 1 ), 0 )
        );
        return getWorkflow( activities, edges, true, false, 1 );
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

}

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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.polypheny.db.util.graph.TopologicalOrderIterator;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.WorkflowImpl;
import org.polypheny.db.workflow.models.ActivityModel;
import org.polypheny.db.workflow.models.EdgeModel;
import org.polypheny.db.workflow.models.WorkflowConfigModel;
import org.polypheny.db.workflow.models.WorkflowModel;

public class WorkflowUtils {

    public static Workflow getWorkflow( List<ActivityModel> activities, List<EdgeModel> edges ) {
        WorkflowConfigModel config = WorkflowConfigModel.of();
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
        return getWorkflow( activities, edges );
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
        return getWorkflow( activities, edges );
    }


    public static List<UUID> getTopologicalActivityIds( Workflow workflow ) {
        List<UUID> list = new ArrayList<>();
        for ( UUID n : TopologicalOrderIterator.of( workflow.toDag() ) ) {
            list.add( n );
        }
        return list;
    }

}

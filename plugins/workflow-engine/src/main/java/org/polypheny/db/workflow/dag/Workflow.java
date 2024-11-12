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

package org.polypheny.db.workflow.dag;

import java.util.List;
import java.util.Map;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.edges.Edge;
import org.polypheny.db.workflow.models.WorkflowModel;

/**
 * Represents an opened workflow that can be edited and executed.
 */
public interface Workflow {

    List<Activity> getActivities(); // TODO: change return type to Map<UUID, Activity> ?

    List<Edge> getEdges();

    Map<String, Object> getConfig(); // TODO: change from object to custom ConfigValue interface.


    /**
     * Returns a WorkflowModel corresponding to the static representation of this workflow.
     * It is used for persistently storing workflows.
     *
     * @return WorkflowModel corresponding to this workflow
     */
    default WorkflowModel toModel() {
        return new WorkflowModel( getActivities().stream().map( Activity::toModel ).toList(),
                getEdges().stream().map( Edge::toModel ).toList(),
                getConfig() );
    }

}

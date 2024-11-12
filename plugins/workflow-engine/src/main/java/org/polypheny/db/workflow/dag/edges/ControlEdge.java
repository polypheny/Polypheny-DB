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

package org.polypheny.db.workflow.dag.edges;

import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.models.EdgeModel;

public class ControlEdge extends Edge {
    private final boolean onSuccess;


    public ControlEdge( Activity from, Activity to, boolean onSuccess ) {
        super( from, to );
        this.onSuccess = onSuccess;
    }


    public EdgeModel toModel() {
        int fromPort = onSuccess ? 0 : 1;
        return new EdgeModel( from.getId(), to.getId(), fromPort, 0, false );
    }

}

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

package org.polypheny.db.catalog.snapshot;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.NonNull;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.logistic.Pattern;

public interface LogicalGraphSnapshot {

    /**
     * Returns an existing graph.
     *
     * @param id The id of the graph to return
     * @return The graph entity with the provided id
     */
    @NonNull Optional<LogicalGraph> getGraph( long id );


    /**
     * Get a collection of all graphs, which match the given conditions.
     *
     * @param graphName The pattern to which the name has to match, null if every name is matched
     * @return A collection of all graphs matching
     */
    @NonNull List<LogicalGraph> getGraphs( @Nullable Pattern graphName );


}

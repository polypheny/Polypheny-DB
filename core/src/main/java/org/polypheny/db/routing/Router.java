/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.routing;

import java.util.List;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.tools.RoutedRelBuilder;
import org.polypheny.db.transaction.Statement;


public interface Router {

    /**
     * @param relRoot the relRoot which will be routed.
     * @param statement the corresponding statement.
     * @param queryInformation different query information resulting from analyze step.
     * @return Proposes multiple routed rel nodes as a List of  relBuilders.
     */
    List<RoutedRelBuilder> route( RelRoot relRoot, Statement statement, LogicalQueryInformation queryInformation );

    /**
     * Resets the routing caches, if some are used.
     */
    void resetCaches();
}



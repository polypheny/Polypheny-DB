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

package org.polypheny.db.routing;

import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.core.lpg.LpgAlg;
import org.polypheny.db.tools.RoutedAlgBuilder;
import org.polypheny.db.transaction.Statement;


public interface Router {

    /**
     * @param algRoot The algRoot which will be routed.
     * @param context
     * @return Proposes multiple routed alg nodes as a List of  relBuilders.
     */
    List<RoutedAlgBuilder> route( AlgRoot algRoot, RoutingContext context );

    /**
     * Resets the routing caches, if some are used.
     */
    void resetCaches();

    <T extends AlgNode & LpgAlg> AlgNode routeGraph( RoutedAlgBuilder builder, T alg, Statement statement );

    AlgNode routeDocument( RoutedAlgBuilder builder, AlgNode alg, Statement statement );

}



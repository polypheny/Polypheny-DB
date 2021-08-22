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

import org.polypheny.db.rel.RelNode;
import org.polypheny.db.tools.RoutedRelBuilder;
import org.polypheny.db.transaction.Statement;

/**
 * Interface for routing dml queries.
 */
public interface DmlRouter {

    /**
     * routes dml queries and returns a rel builder.
     */
    RoutedRelBuilder routeDml( RelNode node, Statement statement );

    /**
     * routes conditional executes and directly returns a rel node.
     */
    RelNode handleConditionalExecute( RelNode node, Statement statement, LogicalQueryInformation queryInformation );

}

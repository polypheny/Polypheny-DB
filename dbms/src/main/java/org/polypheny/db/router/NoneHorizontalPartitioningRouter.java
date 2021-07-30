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

package org.polypheny.db.router;

import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.tools.RoutedRelBuilder;
import org.polypheny.db.transaction.Statement;

@Slf4j
public abstract class NoneHorizontalPartitioningRouter extends AbstractDqlRouter {


    @Override
    protected List<RoutedRelBuilder> handleHorizontalPartitioning( RelNode node, CatalogTable catalogTable, Statement statement, LogicalTable logicalTable, List<RoutedRelBuilder> builders, RelOptCluster cluster ) {
        this.cancelQuery = true;
        return Collections.emptyList();
    }

}

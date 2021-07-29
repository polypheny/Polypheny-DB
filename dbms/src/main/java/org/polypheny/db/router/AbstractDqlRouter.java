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

import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.core.ConditionalExecute;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.Statement;

/**
 * Override buildSelect to adjust default behavior.
 * Other opportunity is to only override handleHorizontalPartitioning and/or handleNoneHorizontalPartitioning
 */
@Slf4j
public abstract class AbstractDqlRouter extends AbstractRouter {

    /**
     * Override route to ignore dml and ConditionalExecutes.
     */
    @Override
    public List<RelRoot> route( RelRoot logicalRoot, Statement statement, ExecutionTimeMonitor executionTimeMonitor ) {
        this.executionTimeMonitor = executionTimeMonitor;

        log.info( "Start Routing" );

        if ( logicalRoot.rel instanceof LogicalTableModify ) {
            return Collections.emptyList();
        } else if ( logicalRoot.rel instanceof ConditionalExecute ) {
            return Collections.emptyList();
        }
        log.info( "Start build DQL" + this.getClass().toString());
        // TODO: get many version
        val builder = RelBuilder.create( statement, logicalRoot.rel.getCluster() );
        List<RelBuilder> builders = buildDql( logicalRoot.rel, Lists.newArrayList(builder), statement, logicalRoot.rel.getCluster() );
        List<RelNode> routed = builders.stream().map( RelBuilder::build ).collect( Collectors.toList() );
        log.info( "End DQL" );

        return routed.stream()
                .map( elem -> new RelRoot( elem, logicalRoot.validatedRowType, logicalRoot.kind, logicalRoot.fields, logicalRoot.collation ) )
                .collect( Collectors.toList() );
    }

}

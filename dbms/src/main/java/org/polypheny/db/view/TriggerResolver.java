/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.view;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.logical.LogicalProcedureExecution;
import org.polypheny.db.algebra.logical.LogicalTableModify;
import org.polypheny.db.algebra.logical.LogicalTriggerExecution;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTrigger;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class TriggerResolver {

    private AlgNode logicalProcedureToTableModify(LogicalProcedureExecution logicalProcedure) {
        return logicalProcedure.getInput();
    }

    private List<CatalogTrigger> searchTriggers(Long tableId){
        return Catalog.getInstance()
                .getTriggers()
                .stream().filter(trigger -> trigger.getTableId() == tableId)
                .collect(Collectors.toList());

    }
    public Optional<LogicalTriggerExecution> lookupTriggers(AlgRoot logicalRoot) {
        Long tableId = logicalRoot.alg.getTable().getTable().getTableId();
        List<AlgNode> algNodes = searchTriggers(tableId)
                .stream()
                .map(CatalogTrigger::getDefinition)
                .collect(Collectors.toList());
        if(algNodes.size() > 0) {
        return Optional.of(LogicalTriggerExecution.create(logicalRoot.alg.getCluster(), (LogicalTableModify) logicalRoot.alg, algNodes, true));
        } else {
            return Optional.empty();
        }
    }
}

/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.transaction.mvcc.rewriting;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelIdentifier;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.transaction.mvcc.IdentifierUtils;

public class RelInsertMod implements AlgTreeModification<LogicalRelModify, LogicalRelModify> {

    @Override
    public LogicalRelModify apply( LogicalRelModify modify ) {
        AlgNode input = modify.getInput();
        if ( input instanceof LogicalRelProject project ) {
            input = rewriteProject( project );
        }
        LogicalRelIdentifier identifier = LogicalRelIdentifier.create(
                modify.getEntity(),
                input,
                input.getTupleType()
        );
        return modify.copy( modify.getTraitSet(), List.of( identifier ) );
    }


    public LogicalRelProject rewriteProject( LogicalRelProject project ) {
        List<AlgDataTypeField> oldFields = project.getInput().getTupleType().getFields();
        List<RexNode> oldProjects = project.getProjects();  // Use projects for field names
        List<String> newFieldNames = new ArrayList<>();
        List<RexNode> newProjects = new ArrayList<>();

        Map<Integer, Integer> indexMapping = new HashMap<>();
        int newIndex = 0;
        for (int i = 0; i < oldFields.size(); i++) {
            if ( IdentifierUtils.isIdentifier( oldFields.get(i) )) {
                continue;
            }
            indexMapping.put(newIndex, i);
            newIndex++;
        }

        if (newIndex == oldFields.size()) {
            return project;
        }

        for (int i = 0; i < oldProjects.size(); i++) {
            RexNode projectNode = oldProjects.get(i);
            if (projectNode instanceof RexIndexRef indexRef) {
                Integer updatedIndex = indexMapping.get(indexRef.getIndex());
                if (updatedIndex != null) {
                    newProjects.add(new RexIndexRef(updatedIndex, oldFields.get(updatedIndex).getType()));
                    newFieldNames.add(oldFields.get(updatedIndex).getName());
                }
            } else {
                newProjects.add(projectNode);
                newFieldNames.add(oldFields.get(i).getName());
            }
        }

        return LogicalRelProject.create(project.getInput(), newProjects, newFieldNames);
    }
}

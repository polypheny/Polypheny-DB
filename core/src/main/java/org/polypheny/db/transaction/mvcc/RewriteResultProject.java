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

package org.polypheny.db.transaction.mvcc;

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.rex.RexNode;

public class RewriteResultProject implements AlgTreeModification<LogicalRelProject, LogicalRelProject> {

    @Override
    public LogicalRelProject apply( LogicalRelProject node ) {

        List<String> oldFieldNames = node.getRowType().getFieldNames();
        List<String> newFieldNames = new ArrayList<>();
        List<RexNode> projects = new ArrayList<>();

        for ( int i = 0; i < oldFieldNames.size(); i++ ) {
            String fieldName = oldFieldNames.get( i );
            if ( fieldName.equals( IdentifierUtils.IDENTIFIER_KEY ) ) {
                continue;
            }
            if ( fieldName.equals( IdentifierUtils.VERSION_KEY ) ) {
                continue;
            }
            newFieldNames.add( fieldName );
            projects.add( node.getProjects().get( i ) );
        }

        return LogicalRelProject.create(
                node.getInput(),
                projects,
                newFieldNames
        );
    }

}

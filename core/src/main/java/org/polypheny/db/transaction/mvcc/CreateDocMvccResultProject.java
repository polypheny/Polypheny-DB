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
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;

public class CreateDocMvccResultProject implements AlgTreeModification<AlgNode, AlgNode>{
    @Override
    public AlgNode apply( AlgNode node ) {
        if (node.getModel() != DataModel.DOCUMENT) {
            throw new IllegalArgumentException("This tree modification is only applicable to document nodes.");
        }

        //TODO: remove this:
        return node;

        /**
        List<AlgDataTypeField> oldFields = node.getTupleType().getFields();
        List<String> newFieldNames = new ArrayList<>();
        List<RexNode> projects = new ArrayList<>();

        for ( int i = 0; i < oldFields.size(); i++ ) {
            AlgDataTypeField currentField = oldFields.get( i );
            if ( currentField.getName().equals( IdentifierUtils.IDENTIFIER_KEY ) ) {
                continue;
            }
            if ( currentField.getName().equals( IdentifierUtils.VERSION_KEY ) ) {
                continue;
            }
            newFieldNames.add( currentField.getName() );
            projects.add( new RexIndexRef( i, currentField.getType() ) );
        }

        return LogicalRelProject.create(
                node,
                projects,
                newFieldNames
        );
         **/
    }
}

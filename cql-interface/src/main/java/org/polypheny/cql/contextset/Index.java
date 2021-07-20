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

package org.polypheny.cql.contextset;

import java.util.HashMap;
import org.polypheny.cql.contextset.exceptions.RelationNotFoundException;
import org.polypheny.cql.contextset.utils.Tree;

public abstract class Index {

    private final HashMap<String, IRelation> relations = new HashMap<>();

    public Tree<BooleanOperator, BooleanExpression> getBooleanTree( String relationName, String searchTerm )
            throws RelationNotFoundException {
        IRelation relation = relations.get( relationName );

        if ( relation == null ) {
            throw new RelationNotFoundException( "No relation defined for given relation name: " + relationName );
        }
        return relation.CreateBooleanTree( searchTerm );
    }

}

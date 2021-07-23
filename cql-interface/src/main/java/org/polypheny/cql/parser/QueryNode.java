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

package org.polypheny.cql.parser;

import lombok.SneakyThrows;
import org.polypheny.cql.utils.Tree;
import org.polypheny.cql.contextset.exceptions.UnexpectedTypeException;

public class QueryNode extends Tree<BooleanGroup, SearchClause> {

    public QueryNode( QueryNode left, BooleanGroup booleanGroup, QueryNode right ) {
        super( left, booleanGroup, right );
    }


    public QueryNode( SearchClause searchClause ) {
        super( searchClause );
    }


    public SearchClause getSearchClause() throws UnexpectedTypeException {
        return super.getExternalNode();
    }


    public BooleanGroup getBooleanGroup() throws UnexpectedTypeException {
        return super.getInternalNode();
    }


    @SneakyThrows
    @Override
    public String toString() {
        return (isLeaf() ? getExternalNode().toString() : getInternalNode().toString());
    }

}

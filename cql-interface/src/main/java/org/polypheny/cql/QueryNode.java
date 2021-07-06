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

package org.polypheny.cql;

public class QueryNode {

    public final BooleanGroup booleanGroup;
    public final SearchClause searchClause;
    public final QueryNode left;
    public final QueryNode right;

    private final boolean leaf;


    public QueryNode( QueryNode left, BooleanGroup booleanGroup, QueryNode right ) {
        this.booleanGroup = booleanGroup;
        this.searchClause = null;
        this.left = left;
        this.right = right;
        this.leaf = false;
    }


    public QueryNode( SearchClause searchClause ) {
        this.booleanGroup = null;
        this.searchClause = searchClause;
        this.left = null;
        this.right = null;
        this.leaf = true;
    }


    public boolean isLeaf() {
        return this.leaf;
    }


    @Override
    public String toString() {
        return (isLeaf() ? searchClause.toString() : booleanGroup.toString());
    }

}

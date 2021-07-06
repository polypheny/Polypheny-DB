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

public class SearchClause {

    public final String index;
    public final Relation relation;
    public final String searchTerm;

    private final boolean termOnlyClause;


    public SearchClause( String index, Relation relation, String searchTerm ) {
        this.index = index;
        this.relation = relation;
        this.searchTerm = searchTerm;
        this.termOnlyClause = false;
    }


    public SearchClause( String searchTerm ) {
        this.index = null;
        this.relation = null;
        this.searchTerm = searchTerm;
        this.termOnlyClause = true;
    }


    public boolean isTermOnlyClause() {
        return this.termOnlyClause;
    }


    @Override
    public String toString() {
        if ( isTermOnlyClause() ) {
            return searchTerm + " ";
        } else {
            return index + " " + relation.toString() + " " + searchTerm;
        }
    }

}

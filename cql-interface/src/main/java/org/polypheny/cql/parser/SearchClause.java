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

public class SearchClause {

    public final String indexStr;
    public final Relation relation;
    public final String searchTerm;

    private final FilterType filterType;
    private final boolean termOnlyClause;


    private SearchClause( String indexStr, Relation relation, String searchTerm, final FilterType filterType ) {
        this.indexStr = indexStr;
        this.relation = relation;
        this.searchTerm = searchTerm;
        this.filterType = filterType;
        this.termOnlyClause = false;
    }


    private SearchClause( String searchTerm ) {
        this.indexStr = null;
        this.relation = null;
        this.searchTerm = searchTerm;
        this.filterType = FilterType.NONE;
        this.termOnlyClause = true;
    }


    public boolean isTermOnlyClause() {
        return this.termOnlyClause;
    }


    public boolean isColumnFilter() {
        return this.filterType == FilterType.COLUMN_FILTER;
    }


    public boolean isLiteralFilter() {
        return this.filterType == FilterType.LITERAL_FILTER;
    }


    @Override
    public String toString() {
        if ( isTermOnlyClause() ) {
            return searchTerm + " ";
        } else {
            return indexStr + " " + relation.toString() + " " + searchTerm;
        }
    }


    public static SearchClause createColumnFilterSearchClause( String index, Relation relation, String searchTerm ) {
        return new SearchClause( index, relation, searchTerm, FilterType.COLUMN_FILTER );
    }


    public static SearchClause createLiteralFilterSearchClause( String index, Relation relation, String searchTerm ) {
        return new SearchClause( index, relation, searchTerm, FilterType.LITERAL_FILTER );
    }


    public static SearchClause createSearchTermOnlySearchClause( String searchTerm ) {
        return new SearchClause( searchTerm );
    }


    private enum FilterType {
        LITERAL_FILTER,
        COLUMN_FILTER,
        NONE;
    }

}

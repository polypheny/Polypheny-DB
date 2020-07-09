/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.restapi.models.requests;


import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.restapi.RequestColumn;
import org.polypheny.db.restapi.exception.IllegalColumnException;
import org.polypheny.db.sql.SqlAggFunction;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.util.Pair;


/**
 * FIXME: LOREM IPSUM
 * <p>
 * General formatting parts:
 * <ul>
 *     <li><code>&lt;qualCol&gt; := &lt;schema&gt;.&lt;table&gt;.&lt;column&gt;</code>: Fully qualified column</li>
 *     <li><code>&lt;qualColOrAlias&gt; := qualCol | &lt;alias&gt;</code>: Fully qualified column or column alias from projection</li>
 *     <li><code>&lt;SOP&gt; := = | != | &lt; | &lt;= | &gt; | &gt= | % | !% </code></li>
 * </ul>
 */
@Slf4j
public class RequestInfo {

    /**
     * The user sending this request.
     * <p>
     * Authenticated via Basic Auth.
     * <p>
     * Currently PDB doesn't really use authentication, but I want to have the infrastructure set up for when this'll change.
     */
    @Getter
    @Setter
    private CatalogUser authenticatedUser;

    /**
     * List of tables affected by this request.
     * <p>
     * Possible values:
     * <p><ul>
     * <li><code>null</code>: This means a list of available tables is requested.</li>
     * <li><code>[table]</code>: Single table to be worked with.</li>
     * <li><code>[table, ...]</code>: Multiple tables, only for GET requests.</li>
     * </ul><p>
     * Formatted as a comma separated list in the URL: <code>/res/[&lt;schema1.table1&gt;[,&lt;schemaN.tableN&gt; ...]</code>
     */
    @Getter
    private List<CatalogTable> tables;

    // Helper data structures constructed from tables

    /**
     * Helps to compute the position of columns in a query.
     */
    private Map<Long, Integer> tableOffsets;

    /**
     * Set containing all valid column IDs for the request. Used to validate things like filters.
     */
    private Set<Long> validColumns;


    /**
     * Literal filters for a request.
     * <p>
     * Used for both GET requests as well as PATCH and DELETE requests.
     * <p>
     * Formatted as URL parameter: <code>?qalColOrAlias=&lt;SOP&gt;&lt;literalValue&gt;</code>
     */
    @Getter
    @Setter
    private Map<RequestColumn, List<Pair<SqlOperator, Object>>> literalFilters;

    /**
     * Column filters for a request.
     *
     * <p>
     * Formatted as URL parameter: <code>?qalColOrAlias=&lt;SOP&gt;&lt;qualColOrAlias&gt;</code>
     */
    @Getter
    @Setter
    private Map<RequestColumn, List<Pair<SqlOperator, RequestColumn>>> columnFilters;

    // Projections, Aliases, Aggregates, Groupings

    /**
     * Projections for result.
     * <p>
     * Formatted as URL parameter: <code>?_project=[&lt;qualCol [@&lt;alias&gt; [(&lt;aggFun&gt;)]]]</code>
     */
    @Getter
    private Pair<List<RequestColumn>, List<String>> projection;
    /**
     * Aggregation functions.
     * <p>
     * See {@link #projection} for formatting.
     */
    @Getter
    @Setter // FIXME: Proper setter function
    private List<Pair<RequestColumn, SqlAggFunction>> aggregateFunctions;
    /**
     * Group by columns.
     * <p>
     * Formatted as URL parameter: <code>?_groupby=[&lt;qualColOrAlias&gt; [,&lt;qualColOrAlias&gt; ...]]</code>
     */
    @Getter
    @Setter // FIXME: Proper setter function
    private List<RequestColumn> groupings;

    private Map<String, RequestColumn> nameAndAliasMapping;

    // Sorting
    /**
     * Limit number of returned results.
     * <p>
     * Formatted as URL parameter: <code>?_limit=&lt;limit&gt;</code>
     */
    @Getter
    @Setter
    private int limit;
    /**
     * Offset returned results.
     * <p>
     * Formatted as URL parameter: <code>?_offset=&lt;offset&gt;</code>
     */
    @Getter
    @Setter
    private int offset;

    /**
     * Sorting for the result.
     * <p>
     * Formatted as URL parameter: <code>?_sort=&lt;qualColOrAlias&gt;[@ ASC | DESC] [, ...]</code>
     */
    @Getter
    @Setter // FIXME: Proper setter function
    private List<Pair<RequestColumn, Boolean>> sort;


    /**
     * Values to insert or update.
     * <p>
     * Formatted in body as JSON.
     */
    @Getter
    @Setter // FIXME: Does this need a proper setter function?
    private List<List<Pair<CatalogColumn, Object>>> values;

    public RequestInfo() {
        // FIXME: Do I need anything here?
    }


    public int getColumnPosition( CatalogColumn column ) {
        return this.tableOffsets.get( column.tableId ) + column.position - 1;
    }


    public RequestColumn getColumnForAlias( String alias ) {
        return this.nameAndAliasMapping.get( alias );
    }


    // Getter and Setter


    /**
     * Sets new tables list and also updates the column offsets.
     * @param tables new table list
     */
    public void setTables( List<CatalogTable> tables ) {
        this.tables = tables;
        this.tableOffsets = new HashMap<>();
        this.validColumns = new HashSet<>();
        this.nameAndAliasMapping = new HashMap<>();
        int columnOffset = 0;
        for ( CatalogTable table : this.tables ) {
            this.tableOffsets.put( table.id, columnOffset );
            this.validColumns.addAll( table.columnIds );
            columnOffset += table.columnIds.size();

        }
    }


    /**
     * Sets literal filters. Validates whether all columns are part of the query.
     * @param literalFilters new literal filters
     * @throws IllegalColumnException thrown if a column is not part of the query
     */
    /*public void setLiteralFilters( Map<RequestColumn, List<Pair<SqlOperator, Object>>> literalFilters ) throws IllegalColumnException {
        for ( RequestColumn column : literalFilters.keySet() ) {
            if ( ! this.validColumns.contains( column.id ) ) {
                // FIXME: Logging
                throw new IllegalColumnException( column );
            }
        }
        this.literalFilters = literalFilters;
    }*/

    /**
     * Sets column filters. Validates whether all columns are part of the query.
     * @param columnFilters new column filters
     * @throws IllegalColumnException thrown if a column is not part of the query
     */
    /*public void setColumnFilters( Map<CatalogColumn, List<Pair<SqlOperator, CatalogColumn>>> columnFilters ) throws IllegalColumnException {
        for ( Entry<CatalogColumn, List<Pair<SqlOperator, CatalogColumn>>> specificColumnFilters : columnFilters.entrySet() ) {
            if ( ! this.validColumns.contains( specificColumnFilters.getKey().id ) ) {
                // FIXME: Logging
                throw new IllegalColumnException( specificColumnFilters.getKey() );
            }

            for ( Pair<SqlOperator, CatalogColumn> innerColumn : specificColumnFilters.getValue() ) {
                if ( ! this.validColumns.contains( innerColumn.right.id ) ) {
                    // FIXME: Logging
                    throw new IllegalColumnException( innerColumn.right );
                }
            }
        }

        this.columnFilters = columnFilters;
    }*/


    /**
     * Set projections. Validates columns. Computes support structures.
     * @param projection new projections
     * @throws IllegalColumnException thrown if a column is not part of the query
     */
    /*public void setProjection( Pair<List<CatalogColumn>, List<String>> projection ) throws IllegalColumnException {
        Map<String, CatalogColumn> nameAndAliasMapping = new HashMap<>();
        for ( Pair<CatalogColumn, String> proj : Pair.zip( projection.left, projection.right ) ) {
            if ( ! this.validColumns.contains( proj.left.id ) ) {
                // FIXME: Logging
                throw new IllegalColumnException( proj.left );
            }

            nameAndAliasMapping.put( proj.left.schemaName + "." + proj.left.tableName + "." + proj.left.name, proj.left );
            if ( proj.right != null ) {
                nameAndAliasMapping.put( proj.right, proj.left );
            }
        }

        this.projection = projection;
        this.nameAndAliasMapping.putAll( nameAndAliasMapping );
//        this.nameAndAliasMapping = nameAndAliasMapping;
    }*/


    /**
     * Returns an immutable copy of the alias mapping.
     * @return the alias mapping
     */
    public Map<String, CatalogColumn> getNameAndAliasMapping() {
        return ImmutableMap.copyOf( this.nameAndAliasMapping );
    }

    public void initialNameMapping( Map<String, CatalogColumn> nameMapping ) {
        this.nameAndAliasMapping.putAll( nameMapping );
    }
}

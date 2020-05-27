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


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.util.Pair;


public class ResourceRequest {

    public ResourceRequest( List<CatalogTable> tables, Pair<List<CatalogColumn>, List<String>> projection, Map<CatalogColumn, List<Pair<SqlOperator, Object>>> filters, int limit, int offset, List<Pair<CatalogColumn, SqlOperator>> sort ) {
        this.tables = tables;
        this.projection = projection;
        this.filters = filters;
        this.limit = limit;
        this.offset = offset;
        this.sort = sort;

        this.tableOffsets = new HashMap<>();
        int columnOffset = 0;
        for ( int i = 0; i < this.tables.size(); i++ ) {
            this.tableOffsets.put( this.tables.get( i ).id, columnOffset );
            columnOffset += this.tables.get( i ).columnIds.size();
        }
    }
    // Formatting:s
    // /restapi/v1/res/<table1>,<table2>,...?<query>

    // <table1>,<table2>,<table3>,<table4>
    public List<CatalogTable> tables;
    private Map<Long, Integer> tableOffsets;

    // ?_project=<column1>,<column2>,<column3>@<alias>
    public Pair<List<CatalogColumn>, List<String>> projection;
    public Map<CatalogColumn, List<Pair<SqlOperator, Object>>> filters;

    // ?_limit=<limit>
    public int limit;
    // ?_offset=<offset>
    public int offset;

    public List<Pair<CatalogColumn, SqlOperator>> sort;


    public int getInputPosition( CatalogColumn column ) {
        return this.tableOffsets.get( column.tableId ) + column.position - 1;
    }


    public static ResourceRequest fromRequest( String resources, String query ) {


        return null;
    }

    private static List<String> stuff() {
        return null;
    }
}

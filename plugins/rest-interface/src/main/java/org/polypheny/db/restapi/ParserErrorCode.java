/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.restapi;


public enum ParserErrorCode {

    GENERIC( 0, "none", "Generic", "Something went wrong. We don't really know what though." ),

    TABLE_LIST_GENERIC( 1000, "table_list", "GenericTableList", "Something went wrong while parsing the table list." ),
    TABLE_LIST_MALFORMED_TABLE( 1001, "table_list", "MalformedTableName", "The table name provided by the user is malformed." ),
    TABLE_LIST_UNKNOWN_TABLE( 1002, "table_list", "UnknownTable", "The provided table is unknown." ),
    //    TABLE_LIST_( 1000, "table_list", "", "")
    PROJECTION_GENERIC( 2000, "projection", "GenericProjection", "Something went wrong while parsing projections." ),
    PROJECTION_INTERNAL( 2001, "projection", "InternalProjection", "Something went wrong internally. Please report this immediately." ),
    PROJECTION_MALFORMED( 2002, "projection", "MalformedColumnName", "The column name provided by the user is malformed." ),
    PROJECTION_UNKNOWN_COLUMN( 2003, "projection", "UnknownColumn", "The provided column is unknown." ),
    PROJECTION_INVALID_COLUMN( 2004, "projection", "InvalidColumn", "The provided column is not part of the provided tables." ),

    LIMIT_GENERIC( 3000, "limit", "GenericLimit", "Something went wrong while parsing limit." ),
    LIMIT_MALFORMED( 3001, "limit", "MalformedLimit", "The limit provided by the user is malformed." ),

    OFFSET_GENERIC( 4000, "offset", "GenericOffset", "Something went wrong while parsing offset." ),
    OFFSET_MALFORMED( 4001, "offset", "MalformedOffset", "The offset provided by the user is malformed." ),

    GROUPING_GENERIC( 5000, "grouping", "GenericGrouping", "Something went wrong while parsing groupings." ),
    GROUPING_UNKNOWN( 5001, "grouping", "UnknownGrouping", "The provided column or alias for grouping is unknown." ),

    SORT_GENERIC( 6000, "sort", "GenericSort", "Something went wrong while parsing sort." ),
    SORT_INTERNAL( 6001, "sort", "InternalSort", "Something went wrong internally." ),
    SORT_MALFORMED( 6002, "sort", "MalformedSort", "The sort provided by the user is malformed." ),
    SORT_MALFORMED_COLUMN( 6003, "sort", "MalformedColumnSort", "The sort column provided by the user is malformed." ),
    SORT_MALFORMED_DIRECTION( 6004, "sort", "MalformedDirectionSort", "The sort direction provided by the user is malformed." ),
    SORT_UNKNOWN_COLUMN( 6005, "sort", "UnknownColumnSort", "The column provided by the user is unknown." ),

    FILTER_GENERIC( 7000, "filter", "GenericFilter", "Something went wrong while parsing a filter." ),
    FILTER_INTERNAL( 7001, "filter", "InternalFilter", "Something went wrong internally." ),
    FILTER_UNKNOWN_COLUMN( 7002, "filter", "UnknownColumnFilter", "The column provided by the user is unknown." ),

    VALUE_GENERIC( 8000, "value", "GenericValue", "Something went wrong while parsing values." ),
    VALUE_INTERNAL( 8001, "value", "InternalValue", "Something went wrong internally." ),
    VALUE_MISSING( 8002, "value", "MissingValue", "The values statement is missing." ),
    VALUE_UNKNOWN_COLUMN( 8003, "value", "UnknownColumnValue", "The column provided by the user is unknown." ),
    VALUE_( 8004, "value", "", "" ),
    ;

    public final int code;
    public final String subsystem;
    public final String name;
    public final String description;


    ParserErrorCode( int code, String subsystem, String name, String description ) {
        this.code = code;
        this.subsystem = subsystem;
        this.name = name;
        this.description = description;
    }
}

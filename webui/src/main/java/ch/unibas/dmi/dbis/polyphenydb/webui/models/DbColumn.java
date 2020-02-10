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

package ch.unibas.dmi.dbis.polyphenydb.webui.models;


/**
 * Information about a column of a table for the header of a table in the UI
 */
public class DbColumn {

    public String name;

    // for both
    public String dataType; //varchar/int/etc

    // for the Data-Table in the UI
    public SortState sort;
    public String filter;

    // for editing columns
    public boolean primary;
    public boolean nullable;
    public Integer maxLength;
    public String defaultValue;


    public DbColumn( final String name ) {
        this.name = name;
    }


    public DbColumn( final String name, final String dataType, final boolean nullable, final Integer maxLength, final SortState sort, final String filter ) {
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
        if ( dataType.equals( "varchar" ) ) {
            this.maxLength = maxLength;
        }
        this.sort = sort;
        this.filter = filter;
    }


    public DbColumn( final String name, final String dataType, final boolean nullable, final Integer maxLength, final boolean primary, final String defaultValue ) {
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
        this.maxLength = maxLength;
        this.primary = primary;
        this.defaultValue = defaultValue;
    }

}

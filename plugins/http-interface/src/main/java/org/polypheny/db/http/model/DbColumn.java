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

package org.polypheny.db.http.model;


import lombok.Setter;
import lombok.experimental.Accessors;


/**
 * Information about a column of a table for the header of a table in the UI
 */
@Accessors(chain = true)
public class DbColumn {

    public String name;
    @Setter
    public String physicalName;

    // for both
    public String dataType; //varchar/int/etc
    public String collectionsType;

    // for the Data-Table in the UI
    public SortState sort;
    public String filter;

    // for editing columns
    public boolean primary;
    public boolean nullable;
    public Integer precision;
    public Integer scale;
    public String defaultValue;
    public Integer dimension;
    public Integer cardinality;

    //for data source columns
    public String as;


    public DbColumn( final String name ) {
        this.name = name;
    }


    public DbColumn(
            final String name,
            final String dataType,
            final boolean nullable,
            final Integer precision,
            final SortState sort,
            final String filter ) {
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
        if ( dataType.equals( "varchar" ) ) {
            this.precision = precision;
        }
        this.sort = sort;
        this.filter = filter;
    }


    public DbColumn(
            final String name,
            final String dataType,
            final String collectionsType,
            final boolean nullable,
            final Integer precision,
            final Integer scale,
            final Integer dimension,
            final Integer cardinality,
            final boolean primary,
            final String defaultValue ) {
        this.name = name;
        this.dataType = dataType;
        this.collectionsType = collectionsType;
        this.nullable = nullable;
        this.precision = precision;
        this.scale = scale;
        this.dimension = dimension;
        this.cardinality = cardinality;
        this.primary = primary;
        this.defaultValue = defaultValue;
    }


    public DbColumn(
            final String name,
            final String dataType,
            final String collectionsType,
            final boolean nullable,
            final Integer precision,
            final Integer scale,
            final Integer dimension,
            final Integer cardinality,
            final boolean primary,
            final String defaultValue,
            final SortState sort,
            final String filter
    ) {
        this( name, dataType, collectionsType, nullable, precision, scale, dimension, cardinality, primary, defaultValue );
        this.sort = sort;
        this.filter = filter;
    }

}

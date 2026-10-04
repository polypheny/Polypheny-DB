/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet.relational.schema;

import lombok.Getter;

/**
 * User-selectable adapter setting that controls how the Parquet schema
 * is presented/imported into Polypheny
 * FLAT - show each Parquet file as one table
 * NORMALIZED - show nested Parquet structures as multiple relational tables
 */
@Getter
public enum ParquetSchemaMode {
    FLAT( "flat" ),
    NORMALIZED( "normalized" );

    private final String settingValue;


    ParquetSchemaMode( String settingValue ) {
        this.settingValue = settingValue;
    }


    public static ParquetSchemaMode from( String value ) {
        for ( ParquetSchemaMode mode : values() ) {
            if ( mode.settingValue.equalsIgnoreCase( value ) ) {
                return mode;
            }
        }
        return FLAT;
    }
}

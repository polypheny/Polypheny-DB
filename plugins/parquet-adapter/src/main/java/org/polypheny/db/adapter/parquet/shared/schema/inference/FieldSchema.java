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

package org.polypheny.db.adapter.parquet.shared.schema.inference;

import lombok.Getter;
import lombok.Setter;

/**
 *  Information about a single column/field
 */
@Getter
public class FieldSchema {

    private final String sourceName; // original name from the workflow input
    private final int sourceIndex; // for relational input
    @Setter
    private String parquetName; // normalized field name that will actually be used in the Parquet schema
    @Setter
    private ValueSchema valueSchema; // kind of value the field contains


    public FieldSchema( String sourceName, String parquetName, int sourceIndex, boolean repeated, ValueSchema valueSchema ) {
        this.sourceName = sourceName;
        this.parquetName = parquetName;
        this.sourceIndex = sourceIndex;
        this.valueSchema = repeated ? ValueSchema.repeated( valueSchema ) : valueSchema;
    }


    public FieldSchema copy() {
        return new FieldSchema( sourceName, parquetName, sourceIndex, false, valueSchema.copy() );
    }

}

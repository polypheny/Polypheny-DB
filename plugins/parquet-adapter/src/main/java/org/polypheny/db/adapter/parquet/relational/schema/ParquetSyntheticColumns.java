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

/**
 * Reserved synthetic column names used by normalized Parquet tables.
 */
public final class ParquetSyntheticColumns {

    public static final String ROW_ID = "__polypheny_row_id";
    public static final String PARENT_ROW_ID = "__polypheny_parent_row_id";
    public static final String ELEM_ORDINAL = "__polypheny_elem_ordinal";


    private ParquetSyntheticColumns() {
    }

}

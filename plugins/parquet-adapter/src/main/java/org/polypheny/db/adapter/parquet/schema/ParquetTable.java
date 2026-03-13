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

package org.polypheny.db.adapter.parquet.schema;

import org.polypheny.db.adapter.parquet.ParquetSource;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Source;
import java.util.List;

/**
 * Base class for Parquet physical tables.
 */
public class ParquetTable extends PhysicalTable {
    protected final Source source;
    protected List<PolyType> fieldTypes;
    protected final int[] fields;
    protected final ParquetSource parquetSource;

    /**
     * Creates a Parquet table wrapper from a physical table definition.
     */
    ParquetTable( long id, Source source, PhysicalTable table, List<PolyType> fieldTypes, int[] fields, ParquetSource parquetSource ) {
        super(
                id,
                table.allocationId,
                table.logicalId,
                table.name,
                table.columns,
                table.namespaceId,
                table.namespaceName,
                table.uniqueFieldIds,
                table.adapterId );
        this.source = source;
        this.fieldTypes = fieldTypes;
        this.fields = fields;
        this.parquetSource = parquetSource;
    }


    /**
     * Various degrees of table "intelligence".
     * Table execution flavors used by the adapter.
     */
    public enum Flavor {
        SCANNABLE, FILTERABLE, TRANSLATABLE
    }

}



/*
 * Copyright 2019-2024 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.adapter.csv;

import java.util.List;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.util.Source;


/**
 * Base class for table that reads CSV files.
 */
public abstract class CsvTable extends PhysicalTable {

    protected final Source source;
    protected List<CsvFieldType> fieldTypes;
    protected final int[] fields;
    protected final CsvSource csvSource;


    /**
     * Creates a CsvTable.
     */
    CsvTable( long id, Source source, PhysicalTable table, List<CsvFieldType> fieldTypes, int[] fields, CsvSource csvSource ) {
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
        this.csvSource = csvSource;
    }


    /**
     * Various degrees of table "intelligence".
     */
    public enum Flavor {
        SCANNABLE, FILTERABLE, TRANSLATABLE
    }

}

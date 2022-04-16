/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adaptimizer.environment;

import java.util.HashMap;
import java.util.List;
import org.apache.commons.lang3.builder.Builder;

/**
 * Builds Suppliers for records of data.
 */
public class DataRecordSupplierBuilder implements Builder<DataRecordSupplier> {
    private static final int DEFAULT_VAR_LENGTH = 10;

    private final Long tableId;
    private final HashMap<Long, DataColumnOption> options;

    private Integer varLength;

    public DataRecordSupplierBuilder( Long tableId ) {
        this.tableId = tableId;
        this.options = new HashMap<>();
    }

    public void addForeignKeyOption( Long columnId, List<Object> objects, boolean oneToOne) {
        DataColumnOption testDataColumnOption;

        if ( this.options.containsKey( columnId ) ) {
            testDataColumnOption = options.get( columnId );
        } else {
            testDataColumnOption = new DataColumnOption();
        }

        testDataColumnOption.setHasOneToOneRelation( oneToOne );
        testDataColumnOption.setProvidedData( objects );
        testDataColumnOption.setForeignKeyColumn( true );

        this.options.put( columnId, testDataColumnOption );
    }

    public void addPrimaryKeyOption( Long columnId ) {
        DataColumnOption testDataColumnOption;

        if ( this.options.containsKey( columnId ) ) {
            testDataColumnOption = options.get( columnId );
        } else {
            testDataColumnOption = new DataColumnOption();
        }

        testDataColumnOption.setPrimaryKeyColumn( true );

        this.options.put( columnId, testDataColumnOption );
    }

    public void setVarLength( int varLength ) {
        this.varLength = varLength;
    }

    @Override
    public DataRecordSupplier build() {
        return new DataRecordSupplier( tableId, options, ( varLength == null ) ? DEFAULT_VAR_LENGTH : varLength );
    }

}

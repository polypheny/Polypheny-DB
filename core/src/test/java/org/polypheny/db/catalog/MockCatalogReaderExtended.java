/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.catalog;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;


/**
 * Adds some extra tables to the mock catalog. These increase the time and complexity of initializing the catalog and
 * are not used for all tests.
 */
public class MockCatalogReaderExtended extends MockCatalogReaderSimple {

    /**
     * Creates a MockCatalogReader.
     *
     * Caller must then call {@link #init} to populate with data.
     *
     * @param typeFactory Type factory
     * @param caseSensitive case sensitivity
     */
    public MockCatalogReaderExtended( AlgDataTypeFactory typeFactory, boolean caseSensitive ) {
        super( typeFactory, caseSensitive );
    }


    @Override
    public MockCatalogReader init() {
        super.init();

        MockSchema structTypeSchema = new MockSchema( "STRUCT" );
        registerSchema( structTypeSchema );
        final Fixture f = new Fixture( typeFactory );
        final List<CompoundNameColumn> columnsExtended = Arrays.asList(
                new CompoundNameColumn( "", "K0", f.varchar20TypeNull ),
                new CompoundNameColumn( "", "C1", f.varchar20TypeNull ),
                new CompoundNameColumn( "F0", "C0", f.intType ),
                new CompoundNameColumn( "F1", "C1", f.intTypeNull ) );
        final List<CompoundNameColumn> extendedColumns = new ArrayList<>( columnsExtended );
        extendedColumns.add( new CompoundNameColumn( "F2", "C2", f.varchar20Type ) );
        final CompoundNameColumnResolver structExtendedTableResolver = new CompoundNameColumnResolver( extendedColumns, "F0" );
        final MockTable structExtendedTypeTable = MockTable.create(
                this,
                structTypeSchema,
                "T_EXTEND",
                false,
                100,
                structExtendedTableResolver );
        for ( CompoundNameColumn column : columnsExtended ) {
            structExtendedTypeTable.addColumn( column.getName(), column.type );
        }
        registerTable( structExtendedTypeTable );

        return this;
    }

}

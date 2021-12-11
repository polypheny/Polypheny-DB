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


import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.type.PolyType;


/**
 * Registers dynamic tables.
 *
 * Not thread-safe.
 */
public class MockCatalogReaderDynamic extends MockCatalogReader {

    /**
     * Creates a MockCatalogReader.
     *
     * Caller must then call {@link #init} to populate with data.
     *
     * @param typeFactory Type factory
     * @param caseSensitive case sensitivity
     */
    public MockCatalogReaderDynamic( AlgDataTypeFactory typeFactory, boolean caseSensitive ) {
        super( typeFactory, caseSensitive );
    }


    @Override
    public MockCatalogReader init() {
        // Register "DYNAMIC" schema.
        MockSchema schema = new MockSchema( "SALES" );
        registerSchema( schema );

        MockTable nationTable = new MockDynamicTable( this, schema.getCatalogName(), schema.getName(), "NATION", false, 100 );
        registerTable( nationTable );

        MockTable customerTable = new MockDynamicTable( this, schema.getCatalogName(), schema.getName(), "CUSTOMER", false, 100 );
        registerTable( customerTable );

        // CREATE TABLE "REGION" - static table with known schema.
        final AlgDataType intType = typeFactory.createPolyType( PolyType.INTEGER );
        final AlgDataType varcharType = typeFactory.createPolyType( PolyType.VARCHAR );

        MockTable regionTable = MockTable.create( this, schema, "REGION", false, 100 );
        regionTable.addColumn( "R_REGIONKEY", intType );
        regionTable.addColumn( "R_NAME", varcharType );
        regionTable.addColumn( "R_COMMENT", varcharType );
        registerTable( regionTable );

        return this;
    }

}


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

package org.polypheny.db.test.catalog;

import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.type.PolyType;

public class MockCatalogReaderDocument extends MockCatalogReaderSimple {

    /**
     * Creates a MockCatalogReader.
     *
     * Caller must then call {@link #init} to populate with data.
     *
     * @param typeFactory Type factory
     * @param caseSensitive case sensitivity
     */
    public MockCatalogReaderDocument( RelDataTypeFactory typeFactory, boolean caseSensitive ) {
        super( typeFactory, caseSensitive );
    }


    @Override
    public MockCatalogReader init() {
        Fixture fixture = getFixture();

        // Register "SALES" schema.
        MockSchema salesSchema = new MockSchema( "private" );
        registerSchema( salesSchema );

        // Register "EMP" table.
        final MockTable empTable = MockTable.create( this, salesSchema, "secrets", false, 14, null );
        empTable.addColumn( "_id", typeFactory.createPolyType( PolyType.VARCHAR, 24 ) );
        empTable.addColumn( "_data", typeFactory.createPolyType( PolyType.JSON ) );
        registerTable( empTable );

        registerTablesWithRollUp( salesSchema, fixture );
        return this;
    }

}

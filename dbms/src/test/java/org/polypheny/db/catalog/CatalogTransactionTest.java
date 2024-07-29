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
 */

package org.polypheny.db.catalog;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.impl.PolyCatalog;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.type.PolyType;

public class CatalogTransactionTest {


    private static TestHelper helper;


    @BeforeAll
    public static void initClass() {
        helper = TestHelper.getInstance();
    }


    @AfterAll
    public static void tearDown() {
        helper.checkAllTrxClosed();
    }


    @Test
    public void simpleRollbackTest() {
        PolyCatalog catalog = new PolyCatalog();

        long namespaceId = catalog.createNamespace( "test", DataModel.RELATIONAL, false );

        catalog.commit();

        LogicalTable table = catalog.getLogicalRel( namespaceId ).addTable( "testTable", EntityType.ENTITY, true );

        catalog.getLogicalRel( namespaceId ).addColumn( "testCol1", table.id, 1, PolyType.BIGINT, null, null, null, null, null, false, null );

        catalog.commit();

        LogicalColumn id = catalog.getLogicalRel( namespaceId ).addColumn( "testCol4", table.id, 2, PolyType.BIGINT, null, null, null, null, null, true, null );
        catalog.rollback();

        assert (catalog.getSnapshot().rel().getColumn( id.id ).isEmpty());
    }


    @Test
    public void rollbackTest() {
        PolyCatalog catalog = new PolyCatalog();

        long namespaceId = catalog.createNamespace( "test", DataModel.RELATIONAL, false );

        catalog.commit();

        LogicalTable table = catalog.getLogicalRel( namespaceId ).addTable( "testTable", EntityType.ENTITY, true );

        catalog.getLogicalRel( namespaceId ).addColumn( "testCol1", table.id, 1, PolyType.BIGINT, null, null, null, null, null, false, null );
        catalog.getLogicalRel( namespaceId ).addColumn( "testCol2", table.id, 2, PolyType.VARCHAR, null, 2646, 5, 2, 2, true, Collation.CASE_INSENSITIVE );
        // catalog.getLogicalRel( namespaceId ).createColumn( "testCol3", table.id, 3, PolyType.BIGINT, null,null, null, null, null, true, null ); 127

        catalog.commit();

        LogicalColumn id = catalog.getLogicalRel( namespaceId ).addColumn( "testCol4", table.id, 3, PolyType.BIGINT, null, null, null, null, null, true, null );
        catalog.rollback();

        assert (catalog.getSnapshot().rel().getColumn( id.id ).isEmpty());
    }

}

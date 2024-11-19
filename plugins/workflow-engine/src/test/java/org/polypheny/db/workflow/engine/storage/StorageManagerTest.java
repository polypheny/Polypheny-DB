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

package org.polypheny.db.workflow.engine.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.type.PolyType;

class StorageManagerTest {

    private static final UUID sessionId = UUID.randomUUID();


    @BeforeAll
    public static void start() throws SQLException {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    @Test
    void hasValidDefaultStoresTest() {
        StorageManager sm = new StorageManagerImpl( sessionId, Map.of() );

        AdapterManager.getInstance().getStore( sm.getDefaultStore( DataModel.RELATIONAL ) ).orElseThrow();
        AdapterManager.getInstance().getStore( sm.getDefaultStore( DataModel.DOCUMENT ) ).orElseThrow();
        AdapterManager.getInstance().getStore( sm.getDefaultStore( DataModel.GRAPH ) ).orElseThrow();
    }


    @Test
    void createRelCheckpointTest() {
        StorageManager sm = new StorageManagerImpl( sessionId, Map.of() );
        UUID activityId = UUID.randomUUID();
        AlgDataTypeFactory typeFactory = AlgDataTypeFactory.DEFAULT;
        AlgDataType type = typeFactory.builder()
                .add( null, "field0", null, typeFactory.createPolyType( PolyType.INTEGER ) )
                .add( null, "field1", null, typeFactory.createPolyType( PolyType.VARCHAR ) )
                .build();
        RelWriter writer = sm.createRelCheckpoint( activityId, 0, type, null );

        LogicalTable table = Catalog.snapshot().rel().getTable( StorageManagerImpl.REL_PREFIX + sessionId.toString(), activityId + "_0" ).orElseThrow();
        for ( int i = 0; i < type.getFieldCount(); i++ ) {
            AlgDataTypeField field = table.getTupleType().getFields().get( i );
            assertEquals( type.getFields().get( i ).getName(), field.getName() );
            assertEquals( type.getFields().get( i ).getType().getPolyType(), field.getType().getPolyType() );
        }
    }


    @Test
    void dropCheckpoints() {
    }

}

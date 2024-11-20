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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
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
import org.polypheny.db.type.entity.PolyValue;

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
        AlgDataType type = getSampleType();
        List<String> primaryKeys = List.of(type.getFieldNames().get( 0 ));
        RelWriter writer = sm.createRelCheckpoint( activityId, 0, type, primaryKeys,false,null );

        LogicalTable table = Catalog.snapshot().rel().getTable( StorageManagerImpl.REL_PREFIX + sessionId, activityId + "_0" ).orElseThrow();
        for ( int i = 0; i < type.getFieldCount(); i++ ) {
            AlgDataTypeField field = table.getTupleType().getFields().get( i );
            assertEquals( type.getFields().get( i ).getName(), field.getName() );
            assertEquals( type.getFields().get( i ).getType().getPolyType(), field.getType().getPolyType() );
        }

        RelReader reader = (RelReader) sm.readCheckpoint( activityId, 0 );
        assertEquals( primaryKeys, reader.getPkCols() );
    }

    @Test
    void writeAndReadRelCheckpointTest() {
        StorageManager sm = new StorageManagerImpl( sessionId, Map.of() );
        UUID activityId = UUID.randomUUID();
        AlgDataType type = getSampleType();
        List<String> primaryKeys = List.of(type.getFieldNames().get( 0 ));
        RelWriter writer = sm.createRelCheckpoint( activityId, 0, type, primaryKeys,false,null );

        RelReader reader = (RelReader) sm.readCheckpoint( activityId, 0 );
        Iterator<PolyValue[]> it = reader.getIterator();
        System.out.println("Iterating...");
        while ( it.hasNext() ) {
            System.out.println( Arrays.toString( it.next() ) );
        }
        System.out.println("Finished...");

    }


    @Test
    void dropCheckpoints() {
    }

    private AlgDataType getSampleType() {
        AlgDataTypeFactory typeFactory = AlgDataTypeFactory.DEFAULT;
        return typeFactory.builder()
                .add( null, "field0", null, typeFactory.createPolyType( PolyType.INTEGER ) )
                .add( null, "field1", null, typeFactory.createPolyType( PolyType.VARCHAR ) )
                .build();
    }

}

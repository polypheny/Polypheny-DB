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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
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
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointQuery;
import org.polypheny.db.workflow.engine.storage.reader.RelReader;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;

class StorageManagerTest {

    private static final UUID sessionId = UUID.randomUUID();


    @BeforeAll
    public static void start() throws SQLException {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    @Test
    void hasValidDefaultStoresTest() throws Exception {
        try ( StorageManager sm = new StorageManagerImpl( sessionId, Map.of() ) ) {
            AdapterManager.getInstance().getStore( sm.getDefaultStore( DataModel.RELATIONAL ) ).orElseThrow();
            AdapterManager.getInstance().getStore( sm.getDefaultStore( DataModel.DOCUMENT ) ).orElseThrow();
            AdapterManager.getInstance().getStore( sm.getDefaultStore( DataModel.GRAPH ) ).orElseThrow();
        }
    }


    @Test
    void createRelCheckpointTest() throws Exception {
        try ( StorageManager sm = new StorageManagerImpl( sessionId, Map.of() ) ) {
            UUID activityId = UUID.randomUUID();
            AlgDataType type = getSampleType();
            sm.createRelCheckpoint( activityId, 0, type, false, null ).close();

            try ( RelReader reader = (RelReader) sm.readCheckpoint( activityId, 0 ) ) {
                reader.getTupleType();
                for ( int i = 0; i < type.getFieldCount(); i++ ) {
                    AlgDataTypeField field = reader.getTupleType().getFields().get( i );
                    assertEquals( type.getFields().get( i ).getName(), field.getName() );
                    assertEquals( type.getFields().get( i ).getType().getPolyType(), field.getType().getPolyType() );
                }

            }
        }
    }


    @Test
    void writeAndReadRelCheckpointTest() throws Exception {
        try ( StorageManager sm = new StorageManagerImpl( sessionId, Map.of() ) ) {
            UUID activityId = UUID.randomUUID();
            AlgDataType type = getSampleType();
            List<List<PolyValue>> sampleData = getSampleData();

            System.out.println( "Writing..." );
            try ( RelWriter writer = sm.createRelCheckpoint( activityId, 0, type, false, null ) ) {
                for ( List<PolyValue> tuple : sampleData ) {
                    writer.write( tuple );
                }
            }

            System.out.println( "Reading..." );
            try ( RelReader reader = (RelReader) sm.readCheckpoint( activityId, 0 ) ) {
                System.out.println( "\nTuple type of checkpoint: " + reader.getTupleType() );
                assertEquals( sampleData.size(), reader.getRowCount() );
                Iterator<List<PolyValue>> it = reader.getIterator();

                int i = 0;
                while ( it.hasNext() ) {
                    List<PolyValue> tuple = it.next();
                    System.out.println( tuple );
                    assertTupleEquals( tuple, sampleData.get( i++ ) );
                }
                assertEquals( sampleData.size(), i );
            }
            System.out.println( "Finished..." );
        }

    }


    @Test
    void writeWhileReadingRelCheckpointTest() throws Exception {
        try ( StorageManager sm = new StorageManagerImpl( sessionId, Map.of() ) ) {
            UUID activityId1 = UUID.randomUUID();
            UUID activityId2 = UUID.randomUUID();
            AlgDataType type = getSampleType();
            List<List<PolyValue>> sampleData = getSampleData();

            try ( RelWriter writer = sm.createRelCheckpoint( activityId1, 0, type, false, null ) ) {
                System.out.println( "Writing 1..." );
                for ( List<PolyValue> tuple : sampleData ) {
                    System.out.println( tuple );
                    writer.write( tuple );
                }
            }

            try ( RelWriter writer = sm.createRelCheckpoint( activityId2, 0, type, false, null );
                    RelReader reader = (RelReader) sm.readCheckpoint( activityId1, 0 )
            ) {
                System.out.println( "Concurrently reading 1 and writing 2" );
                writer.write( reader.getIterator() );
            }
        }
    }


    @Test
    void readQueryResultFromRelCheckpointTest() throws Exception {
        try ( StorageManager sm = new StorageManagerImpl( sessionId, Map.of() ) ) {
            UUID activityId = UUID.randomUUID();
            AlgDataType type = getSampleType();
            List<List<PolyValue>> sampleData = getSampleData();

            try ( RelWriter writer = sm.createRelCheckpoint( activityId, 0, type, false, null ) ) {
                writer.write( sampleData.iterator() );
            }

            PolyInteger newValue = PolyInteger.of( sampleData.get( 0 ).get( 0 ).asInteger().value + 1 );
            String intField = type.getFieldNames().get( 0 );
            CheckpointQuery query = CheckpointQuery.builder()
                    .queryLanguage( "SQL" )
                    .query( "SELECT * FROM " + CheckpointQuery.ENTITY() + " WHERE " + intField + " > ? ORDER BY " + intField + " DESC" )
                    .parameter( 0, Pair.of( type.getFields().get( 0 ).getType(), newValue ) )
                    .build();

            try ( RelReader reader = (RelReader) sm.readCheckpoint( activityId, 0 ) ) {

                int i = sampleData.size() - 1;  // reverse order because of DESC
                Iterator<List<PolyValue>> it = reader.getIteratorFromQuery( query );
                while ( it.hasNext() ) {
                    assertTupleEquals( sampleData.get( i ), it.next() );
                    i--;
                }
            }
        }
    }


    @Test
    void dropCheckpoints() throws Exception {
        AlgDataType type = getSampleType();
        UUID activityId1 = UUID.randomUUID();
        UUID activityId2 = UUID.randomUUID();

        try ( StorageManager sm = new StorageManagerImpl( sessionId, Map.of() ) ) {
            sm.createRelCheckpoint( activityId1, 0, type, false, null ).close();
            sm.createRelCheckpoint( activityId1, 1, type, false, null ).close();

            sm.createRelCheckpoint( activityId2, 0, type, false, null ).close();
            sm.createRelCheckpoint( activityId2, 1, type, false, null ).close();

            sm.dropCheckpoints( activityId1 );
            assertFalse( sm.hasCheckpoint( activityId1, 0 ) );
            assertFalse( sm.hasCheckpoint( activityId1, 1 ) );
            assertTrue( sm.hasCheckpoint( activityId2, 0 ) );
            assertTrue( sm.hasCheckpoint( activityId2, 1 ) );
        }
    }


    private AlgDataType getSampleType() {
        AlgDataTypeFactory typeFactory = AlgDataTypeFactory.DEFAULT;
        return typeFactory.builder()
                .add( null, "field0", null, typeFactory.createPolyType( PolyType.INTEGER ) )
                .add( null, "field1", null, typeFactory.createPolyType( PolyType.VARCHAR ) )
                .build();
    }


    private List<List<PolyValue>> getSampleData() {
        List<List<PolyValue>> tuples = new ArrayList<>();
        tuples.add( List.of( PolyInteger.of( 42 ), PolyString.of( "This is a test" ) ) );
        tuples.add( List.of( PolyInteger.of( 123 ), PolyString.of( "abcd" ) ) );
        tuples.add( List.of( PolyInteger.of( 456 ), PolyString.of( "efgh" ) ) );
        return tuples;
    }


    private void assertTupleEquals( List<PolyValue> t1, List<PolyValue> t2 ) {
        assertEquals( t1.size(), t2.size() );
        for ( int i = 0; i < t1.size(); i++ ) {
            PolyValue v1 = t1.get( i );
            PolyValue v2 = t2.get( i );
            assertEquals( v1.type, v2.type );
            assertEquals( v1.toJson(), v2.toJson() );
        }
    }

}

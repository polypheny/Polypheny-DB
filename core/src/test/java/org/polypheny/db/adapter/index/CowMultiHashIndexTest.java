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

package org.polypheny.db.adapter.index;


import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.polypheny.db.transaction.PUID;
import org.polypheny.db.transaction.PUID.Type;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.util.Pair;


public class CowMultiHashIndexTest {

    @Test
    public void testCopyOnWriteIsolation() {
        // Set up
        CowMultiHashIndex idx = new CowMultiHashIndex( 42L, "idx_test", null, null, Collections.emptyList(), Collections.emptyList() );
        PolyXid xid1 = PolyXid.generateLocalTransactionIdentifier( PUID.randomPUID( Type.NODE ), PUID.randomPUID( Type.TRANSACTION ) );
        PolyXid xid2 = PolyXid.generateLocalTransactionIdentifier( PUID.randomPUID( Type.NODE ), PUID.randomPUID( Type.TRANSACTION ) );
        Assertions.assertEquals( 0, idx.getRaw().size() );
        // Insert and delete some values as xid1
        idx.insert( xid1, CowHashIndexTest.asPolyValues( 1, 2, 3 ), CowHashIndexTest.asPolyValues( 1 ) );
        idx.insertAll( xid1, Arrays.asList(
                Pair.of( CowHashIndexTest.asPolyValues( 2, 3, 4 ), CowHashIndexTest.asPolyValues( 2 ) ),
                Pair.of( CowHashIndexTest.asPolyValues( 2, 3, 4 ), CowHashIndexTest.asPolyValues( 5 ) ),
                Pair.of( CowHashIndexTest.asPolyValues( 3, 4, 5 ), CowHashIndexTest.asPolyValues( 3 ) ),
                Pair.of( CowHashIndexTest.asPolyValues( 4, 5, 6 ), CowHashIndexTest.asPolyValues( 4 ) ),
                Pair.of( CowHashIndexTest.asPolyValues( 4, 5, 6 ), CowHashIndexTest.asPolyValues( 6 ) )
        ) );
        idx.delete( xid1, CowHashIndexTest.asPolyValues( 2, 3, 4 ) );
        // Make sure the values are not yet visible by either transaction
        Assertions.assertFalse( idx.contains( xid1, CowHashIndexTest.asPolyValues( 1, 2, 3 ) ) );
        Assertions.assertFalse( idx.contains( xid1, CowHashIndexTest.asPolyValues( 2, 3, 4 ) ) );
        Assertions.assertFalse( idx.contains( xid1, CowHashIndexTest.asPolyValues( 3, 4, 5 ) ) );
        Assertions.assertFalse( idx.contains( xid2, CowHashIndexTest.asPolyValues( 1, 2, 3 ) ) );
        Assertions.assertFalse( idx.contains( xid2, CowHashIndexTest.asPolyValues( 2, 3, 4 ) ) );
        Assertions.assertFalse( idx.contains( xid2, CowHashIndexTest.asPolyValues( 3, 4, 5 ) ) );
        // Invoke atom isolation barrier
        idx.barrier( xid1 );
        // Make sure the values are only visible by transaction 1
        Assertions.assertTrue( idx.contains( xid1, CowHashIndexTest.asPolyValues( 1, 2, 3 ) ) );
        Assertions.assertFalse( idx.contains( xid1, CowHashIndexTest.asPolyValues( 2, 3, 4 ) ) );
        Assertions.assertTrue( idx.contains( xid1, CowHashIndexTest.asPolyValues( 3, 4, 5 ) ) );
        Assertions.assertFalse( idx.contains( xid2, CowHashIndexTest.asPolyValues( 1, 2, 3 ) ) );
        Assertions.assertFalse( idx.contains( xid2, CowHashIndexTest.asPolyValues( 2, 3, 4 ) ) );
        Assertions.assertFalse( idx.contains( xid2, CowHashIndexTest.asPolyValues( 3, 4, 5 ) ) );
        // Commit
        idx.commit( xid1 );
        // Make sure the values are visible by both transactions
        Assertions.assertTrue( idx.contains( xid1, CowHashIndexTest.asPolyValues( 1, 2, 3 ) ) );
        Assertions.assertFalse( idx.contains( xid1, CowHashIndexTest.asPolyValues( 2, 3, 4 ) ) );
        Assertions.assertTrue( idx.contains( xid1, CowHashIndexTest.asPolyValues( 3, 4, 5 ) ) );
        Assertions.assertTrue( idx.contains( xid2, CowHashIndexTest.asPolyValues( 1, 2, 3 ) ) );
        Assertions.assertFalse( idx.contains( xid1, CowHashIndexTest.asPolyValues( 2, 3, 4 ) ) );
        Assertions.assertTrue( idx.contains( xid2, CowHashIndexTest.asPolyValues( 3, 4, 5 ) ) );
        // Insert, then rollback
        idx.insert( xid1, CowHashIndexTest.asPolyValues( 2, 3, 4 ), CowHashIndexTest.asPolyValues( 1 ) );
        Assertions.assertFalse( idx.contains( xid1, CowHashIndexTest.asPolyValues( 2, 3, 4 ) ) );
        Assertions.assertFalse( idx.contains( xid2, CowHashIndexTest.asPolyValues( 2, 3, 4 ) ) );
        idx.barrier( xid1 );
        Assertions.assertTrue( idx.contains( xid1, CowHashIndexTest.asPolyValues( 2, 3, 4 ) ) );
        Assertions.assertFalse( idx.contains( xid2, CowHashIndexTest.asPolyValues( 2, 3, 4 ) ) );
        idx.rollback( xid1 );
        Assertions.assertFalse( idx.contains( xid1, CowHashIndexTest.asPolyValues( 2, 3, 4 ) ) );
        Assertions.assertFalse( idx.contains( xid2, CowHashIndexTest.asPolyValues( 2, 3, 4 ) ) );
    }


    @Test
    public void testDuplicateInsertion() {
        CowMultiHashIndex idx = new CowMultiHashIndex( 42L, "idx_test", null, null, Collections.emptyList(), Collections.emptyList() );
        PolyXid xid1 = PolyXid.generateLocalTransactionIdentifier( PUID.randomPUID( Type.NODE ), PUID.randomPUID( Type.TRANSACTION ) );
        idx.insert( xid1, CowHashIndexTest.asPolyValues( 1, 2, 3 ), CowHashIndexTest.asPolyValues( 1 ) );
        idx.insert( xid1, CowHashIndexTest.asPolyValues( 1, 2, 3 ), CowHashIndexTest.asPolyValues( 2 ) );
        idx.barrier( xid1 );
        idx.rollback( xid1 );
        idx.insertAll( xid1, Arrays.asList(
                Pair.of( CowHashIndexTest.asPolyValues( 2, 3, 4 ), CowHashIndexTest.asPolyValues( 2 ) ),
                Pair.of( CowHashIndexTest.asPolyValues( 3, 4, 5 ), CowHashIndexTest.asPolyValues( 3 ) ),
                Pair.of( CowHashIndexTest.asPolyValues( 4, 5, 6 ), CowHashIndexTest.asPolyValues( 4 ) ),
                Pair.of( CowHashIndexTest.asPolyValues( 3, 4, 5 ), CowHashIndexTest.asPolyValues( 5 ) ),
                Pair.of( CowHashIndexTest.asPolyValues( 5, 6, 7 ), CowHashIndexTest.asPolyValues( 6 ) )
        ) );
        idx.barrier( xid1 );
        idx.commit( xid1 );
    }


    @Test
    public void testContains() {
        CowMultiHashIndex idx = new CowMultiHashIndex( 42L, "idx_test", null, null, Collections.emptyList(), Collections.emptyList() );
        PolyXid xid1 = PolyXid.generateLocalTransactionIdentifier( PUID.randomPUID( Type.NODE ), PUID.randomPUID( Type.TRANSACTION ) );
        idx.insertAll( xid1, Arrays.asList(
                Pair.of( CowHashIndexTest.asPolyValues( 2, 3, 4 ), CowHashIndexTest.asPolyValues( 2 ) ),
                Pair.of( CowHashIndexTest.asPolyValues( 3, 4, 5 ), CowHashIndexTest.asPolyValues( 3 ) ),
                Pair.of( CowHashIndexTest.asPolyValues( 4, 5, 6 ), CowHashIndexTest.asPolyValues( 4 ) ),
                Pair.of( CowHashIndexTest.asPolyValues( 3, 4, 5 ), CowHashIndexTest.asPolyValues( 5 ) ),
                Pair.of( CowHashIndexTest.asPolyValues( 5, 6, 7 ), CowHashIndexTest.asPolyValues( 6 ) )
        ) );
        idx.barrier( xid1 );
        Assertions.assertTrue( idx.contains( xid1, CowHashIndexTest.asPolyValues( 3, 4, 5 ) ) );
        Assertions.assertFalse( idx.contains( xid1, CowHashIndexTest.asPolyValues( 1, 2, 3 ) ) );
        Assertions.assertTrue( idx.containsAny( xid1, Arrays.asList( CowHashIndexTest.asPolyValues( 1, 2, 3 ), CowHashIndexTest.asPolyValues( 3, 4, 5 ) ) ) );
        Assertions.assertFalse( idx.containsAny( xid1, Arrays.asList( CowHashIndexTest.asPolyValues( 1, 2, 3 ), CowHashIndexTest.asPolyValues( 0, 1, 2 ) ) ) );
        Assertions.assertTrue( idx.containsAll( xid1, Arrays.asList( CowHashIndexTest.asPolyValues( 3, 4, 5 ), CowHashIndexTest.asPolyValues( 5, 6, 7 ) ) ) );
        Assertions.assertFalse( idx.containsAll( xid1, Arrays.asList( CowHashIndexTest.asPolyValues( 1, 2, 3 ), CowHashIndexTest.asPolyValues( 3, 4, 5 ) ) ) );
    }

}

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

package org.polypheny.db.adapter.index;


import java.util.Arrays;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;
import org.polypheny.db.algebra.exceptions.ConstraintViolationException;
import org.polypheny.db.transaction.PUID;
import org.polypheny.db.transaction.PUID.Type;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.util.Pair;


public class CowHashIndexTest {

    @Test
    public void testCopyOnWriteIsolation() {
        // Set up
        CoWHashIndex idx = new CoWHashIndex( 42L, "idx_test", null, null, Collections.emptyList(), Collections.emptyList() );
        PolyXid xid1 = PolyXid.generateLocalTransactionIdentifier( PUID.randomPUID( Type.NODE ), PUID.randomPUID( Type.TRANSACTION ) );
        PolyXid xid2 = PolyXid.generateLocalTransactionIdentifier( PUID.randomPUID( Type.NODE ), PUID.randomPUID( Type.TRANSACTION ) );
        Assert.assertEquals( 0, idx.getRaw().size() );
        // Insert and delete some values as xid1
        idx.insert( xid1, Arrays.asList( 1, 2, 3 ), Collections.singletonList( 1 ) );
        idx.insertAll( xid1, Arrays.asList(
                Pair.of( Arrays.asList( 2, 3, 4 ), Collections.singletonList( 2 ) ),
                Pair.of( Arrays.asList( 3, 4, 5 ), Collections.singletonList( 3 ) ),
                Pair.of( Arrays.asList( 4, 5, 6 ), Collections.singletonList( 4 ) )
        ) );
        idx.delete( xid1, Arrays.asList( 2, 3, 4 ) );
        // Make sure the values are not yet visible by either transaction
        Assert.assertFalse( idx.contains( xid1, Arrays.asList( 1, 2, 3 ) ) );
        Assert.assertFalse( idx.contains( xid1, Arrays.asList( 2, 3, 4 ) ) );
        Assert.assertFalse( idx.contains( xid1, Arrays.asList( 3, 4, 5 ) ) );
        Assert.assertFalse( idx.contains( xid2, Arrays.asList( 1, 2, 3 ) ) );
        Assert.assertFalse( idx.contains( xid2, Arrays.asList( 2, 3, 4 ) ) );
        Assert.assertFalse( idx.contains( xid2, Arrays.asList( 3, 4, 5 ) ) );
        // Invoke atom isolation barrier
        idx.barrier( xid1 );
        // Make sure the values are only visible by transaction 1
        Assert.assertTrue( idx.contains( xid1, Arrays.asList( 1, 2, 3 ) ) );
        Assert.assertFalse( idx.contains( xid1, Arrays.asList( 2, 3, 4 ) ) );
        Assert.assertTrue( idx.contains( xid1, Arrays.asList( 3, 4, 5 ) ) );
        Assert.assertFalse( idx.contains( xid2, Arrays.asList( 1, 2, 3 ) ) );
        Assert.assertFalse( idx.contains( xid2, Arrays.asList( 2, 3, 4 ) ) );
        Assert.assertFalse( idx.contains( xid2, Arrays.asList( 3, 4, 5 ) ) );
        // Commit
        idx.commit( xid1 );
        // Make sure the values are visible by both transactions
        Assert.assertTrue( idx.contains( xid1, Arrays.asList( 1, 2, 3 ) ) );
        Assert.assertFalse( idx.contains( xid1, Arrays.asList( 2, 3, 4 ) ) );
        Assert.assertTrue( idx.contains( xid1, Arrays.asList( 3, 4, 5 ) ) );
        Assert.assertTrue( idx.contains( xid2, Arrays.asList( 1, 2, 3 ) ) );
        Assert.assertFalse( idx.contains( xid1, Arrays.asList( 2, 3, 4 ) ) );
        Assert.assertTrue( idx.contains( xid2, Arrays.asList( 3, 4, 5 ) ) );
        // Insert, then rollback
        idx.insert( xid1, Arrays.asList( 2, 3, 4 ), Collections.singletonList( 1 ) );
        Assert.assertFalse( idx.contains( xid1, Arrays.asList( 2, 3, 4 ) ) );
        Assert.assertFalse( idx.contains( xid2, Arrays.asList( 2, 3, 4 ) ) );
        idx.barrier( xid1 );
        Assert.assertTrue( idx.contains( xid1, Arrays.asList( 2, 3, 4 ) ) );
        Assert.assertFalse( idx.contains( xid2, Arrays.asList( 2, 3, 4 ) ) );
        idx.rollback( xid1 );
        Assert.assertFalse( idx.contains( xid1, Arrays.asList( 2, 3, 4 ) ) );
        Assert.assertFalse( idx.contains( xid2, Arrays.asList( 2, 3, 4 ) ) );
    }


    @Test
    public void testDuplicateDetection() {
        CoWHashIndex idx = new CoWHashIndex( 42L, "idx_test", null, null, Collections.emptyList(), Collections.emptyList() );
        PolyXid xid1 = PolyXid.generateLocalTransactionIdentifier( PUID.randomPUID( Type.NODE ), PUID.randomPUID( Type.TRANSACTION ) );
        idx.insert( xid1, Arrays.asList( 1, 2, 3 ), Collections.singletonList( 1 ) );
        idx.insert( xid1, Arrays.asList( 1, 2, 3 ), Collections.singletonList( 1 ) );
        try {
            idx.barrier( xid1 );
            Assert.fail( "Expected ConstraintViolationException not thrown!" );
        } catch ( ConstraintViolationException ignored ) {
            // pass
        } catch ( Throwable t ) {
            Assert.fail( "Expected ConstraintViolationException, got this instead: " + t );
        }
        idx.rollback( xid1 );
        idx.insertAll( xid1, Arrays.asList(
                Pair.of( Arrays.asList( 2, 3, 4 ), Collections.singletonList( 2 ) ),
                Pair.of( Arrays.asList( 3, 4, 5 ), Collections.singletonList( 3 ) ),
                Pair.of( Arrays.asList( 4, 5, 6 ), Collections.singletonList( 4 ) ),
                Pair.of( Arrays.asList( 3, 4, 5 ), Collections.singletonList( 5 ) ),
                Pair.of( Arrays.asList( 5, 6, 7 ), Collections.singletonList( 6 ) )
        ) );
        try {
            idx.barrier( xid1 );
            Assert.fail( "Expected ConstraintViolationException not thrown!" );
        } catch ( ConstraintViolationException ignored ) {
            // pass
        } catch ( Throwable t ) {
            Assert.fail( "Expected ConstraintViolationException, got this instead: " + t );
        }
        // Attempt to commit, should fail, as barrier could not complete
        try {
            idx.commit( xid1 );
            Assert.fail( "Expected IllegalStateException not thrown!" );
        } catch ( IllegalStateException ignored ) {
            // pass
        } catch ( Throwable t ) {
            Assert.fail( "Expected IllegalStateException, got this instead: " + t );
        }
        // No choice but to roll back
        idx.rollback( xid1 );
    }


    @Test
    public void testContains() {
        CoWHashIndex idx = new CoWHashIndex( 42L, "idx_test", null, null, Collections.emptyList(), Collections.emptyList() );
        PolyXid xid1 = PolyXid.generateLocalTransactionIdentifier( PUID.randomPUID( Type.NODE ), PUID.randomPUID( Type.TRANSACTION ) );
        idx.insertAll( xid1, Arrays.asList(
                Pair.of( Arrays.asList( 2, 3, 4 ), Collections.singletonList( 2 ) ),
                Pair.of( Arrays.asList( 3, 4, 5 ), Collections.singletonList( 3 ) ),
                Pair.of( Arrays.asList( 4, 5, 6 ), Collections.singletonList( 4 ) ),
                Pair.of( Arrays.asList( 5, 6, 7 ), Collections.singletonList( 5 ) )
        ) );
        idx.barrier( xid1 );
        Assert.assertTrue( idx.contains( xid1, Arrays.asList( 3, 4, 5 ) ) );
        Assert.assertFalse( idx.contains( xid1, Arrays.asList( 1, 2, 3 ) ) );
        Assert.assertTrue( idx.containsAny( xid1, Arrays.asList( Arrays.asList( 1, 2, 3 ), Arrays.asList( 3, 4, 5 ) ) ) );
        Assert.assertFalse( idx.containsAny( xid1, Arrays.asList( Arrays.asList( 1, 2, 3 ), Arrays.asList( 0, 1, 2 ) ) ) );
        Assert.assertTrue( idx.containsAll( xid1, Arrays.asList( Arrays.asList( 3, 4, 5 ), Arrays.asList( 5, 6, 7 ) ) ) );
        Assert.assertFalse( idx.containsAll( xid1, Arrays.asList( Arrays.asList( 1, 2, 3 ), Arrays.asList( 3, 4, 5 ) ) ) );
    }

}

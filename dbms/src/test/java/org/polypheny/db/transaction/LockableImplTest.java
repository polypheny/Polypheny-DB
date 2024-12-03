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

package org.polypheny.db.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.transaction.locking.Lockable.LockType;
import org.polypheny.db.transaction.locking.LockableImpl;

public class LockableImplTest {

    private LockableImpl lockable;
    private Transaction transaction1;
    private Transaction transaction2;


    @BeforeEach
    public void setup() {
        transaction1 = new MockTransaction( 1 );
        transaction2 = new MockTransaction( 2 );
        lockable = new LockableImpl( null );
    }

    @Test
    public void isSharedByDefault() {
        assertEquals( LockType.SHARED, lockable.getLockType());
        assertTrue( lockable.getCopyOfOwners().isEmpty() );
    }

    @Test
    public void isExclusiveAfterAcquired() {
        lockable.acquire( transaction1, LockType.EXCLUSIVE );
        assertEquals( LockType.EXCLUSIVE, lockable.getLockType() );
        assertEquals( 1, lockable.getCopyOfOwners().size() );
    }

    @Test
    public void isSharedAfterAcquired() {
        lockable.acquire( transaction1, LockType.SHARED );
        assertEquals( LockType.SHARED, lockable.getLockType());
        assertEquals( 1, lockable.getCopyOfOwners().size() );
    }

    @Test
    public void simpleUpgrade() {
        lockable.acquire( transaction1, LockType.SHARED );
        assertEquals( LockType.SHARED, lockable.getLockType());
        lockable.acquire( transaction1, LockType.EXCLUSIVE );
        assertEquals( LockType.EXCLUSIVE, lockable.getLockType() );
        assertEquals( 1, lockable.getCopyOfOwners().size() );
    }

    @Test
    public void multipleSharedOwners() {
        lockable.acquire( transaction1, LockType.SHARED );
        lockable.acquire( transaction2, LockType.SHARED );
        assertEquals( LockType.SHARED, lockable.getLockType());
        assertEquals( 2, lockable.getCopyOfOwners().size() );
    }

    @Test
    public void releaseSharedSingle() {
        lockable.acquire( transaction1, LockType.SHARED );
        assertEquals( LockType.SHARED, lockable.getLockType());
        assertEquals( 1, lockable.getCopyOfOwners().size() );
        lockable.release( transaction1 );
        assertEquals( LockType.SHARED, lockable.getLockType());
        assertTrue( lockable.getCopyOfOwners().isEmpty() );
    }

    @Test
    public void releaseExclusiveSingle() {
        lockable.acquire( transaction1, LockType.EXCLUSIVE );
        assertEquals( LockType.EXCLUSIVE, lockable.getLockType() );
        assertEquals( 1, lockable.getCopyOfOwners().size() );
        lockable.release( transaction1 );
        assertEquals( LockType.SHARED, lockable.getLockType());
        assertTrue( lockable.getCopyOfOwners().isEmpty() );
    }
}

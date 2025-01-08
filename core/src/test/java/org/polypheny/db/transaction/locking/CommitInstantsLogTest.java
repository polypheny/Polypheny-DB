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

package org.polypheny.db.transaction.locking;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CommitInstantsLogTest {

    private CommitInstantsLog log;

    @BeforeEach
    void setUp() {
        log = new CommitInstantsLog();
    }

    @Test
    void testSetOrUpdateLastCommitAndGetLastCommit() {
        EntryIdentifier identifier = new EntryIdentifier(1L, 1L);
        long instant = 1000L;

        log.setOrUpdateLastCommit(identifier, instant);
        long retrievedInstant = log.getLastCommit(identifier);

        assertEquals(instant, retrievedInstant, "The retrieved instant should match the updated instant.");
    }

    @Test
    void testSetOrUpdateLastCommitOverridesPreviousCommit() {
        EntryIdentifier identifier = new EntryIdentifier(1L, 1L);
        long firstInstant = 1000L;
        long secondInstant = 2000L;

        log.setOrUpdateLastCommit(identifier, firstInstant);
        log.setOrUpdateLastCommit(identifier, secondInstant);
        long retrievedInstant = log.getLastCommit(identifier);

        assertEquals(secondInstant, retrievedInstant, "The retrieved instant should match the most recent update.");
    }

    @Test
    void testGetLastCommitForNonexistentEntry() {
        EntryIdentifier identifier = new EntryIdentifier(1L, 1L);

        long retrievedInstant = log.getLastCommit(identifier);

        assertEquals(CommitInstantsLog.NO_COMMIT_INSTANT, retrievedInstant,
                "The retrieved instant for a nonexistent entry should match NO_COMMIT_INSTANT.");
    }

    @Test
    void testRemoveEntry() {
        EntryIdentifier identifier = new EntryIdentifier(1L, 1L);
        long instant = 1000L;

        log.setOrUpdateLastCommit(identifier, instant);
        log.removeEntry(identifier);

        long retrievedInstant = log.getLastCommit(identifier);

        assertEquals(CommitInstantsLog.NO_COMMIT_INSTANT, retrievedInstant,
                "The retrieved instant after removal should match NO_COMMIT_INSTANT.");
    }

    @Test
    void testRemoveEntryForNonexistentEntry() {
        EntryIdentifier identifier = new EntryIdentifier(1L, 1L);

        assertDoesNotThrow(() -> log.removeEntry(identifier),
                "Removing a nonexistent entry should not throw an exception.");
    }

    @Test
    void testMultipleEntries() {
        EntryIdentifier identifier1 = new EntryIdentifier(1L, 1L);
        long instant1 = 1000L;

        EntryIdentifier identifier2 = new EntryIdentifier(2L, 2L);
        long instant2 = 2000L;

        log.setOrUpdateLastCommit(identifier1, instant1);
        log.setOrUpdateLastCommit(identifier2, instant2);

        assertEquals(instant1, log.getLastCommit(identifier1),
                "The first entry's instant should match its updated value.");
        assertEquals(instant2, log.getLastCommit(identifier2),
                "The second entry's instant should match its updated value.");
    }

}

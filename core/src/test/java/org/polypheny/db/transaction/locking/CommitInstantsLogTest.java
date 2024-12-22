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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CommitInstantsLogTest {
    CommitInstantsLog log;

    @BeforeEach
    void setUp() {
        log = new CommitInstantsLog();
    }

    @Test
    void testSetOrUpdateLastCommitAndGetLastCommit() {
        long entryId = 1L;
        long version = 1L;
        long instant = 1000L;

        log.setOrUpdateLastCommit(entryId, version, instant);
        long retrievedInstant = log.getLastCommit(entryId, version);

        assertEquals(instant, retrievedInstant, "The retrieved instant should match the updated instant.");
    }

    @Test
    void testSetOrUpdateLastCommitOverridesPreviousCommit() {
        long entryId = 1L;
        long version = 1L;
        long firstInstant = 1000L;
        long secondInstant = 2000L;

        log.setOrUpdateLastCommit(entryId, version, firstInstant);
        log.setOrUpdateLastCommit(entryId, version, secondInstant);
        long retrievedInstant = log.getLastCommit(entryId, version);

        assertEquals(secondInstant, retrievedInstant, "The retrieved instant should match the most recent update.");
    }

    @Test
    void testGetLastCommitForNonexistentEntry() {
        long entryId = 1L;
        long version = 1L;

        Exception exception = assertThrows(NullPointerException.class, () -> {
            log.getLastCommit(entryId, version);
        });

        assertNotNull(exception, "An exception should be thrown for a nonexistent entry.");
    }

    @Test
    void testRemoveEntry() {
        long entryId = 1L;
        long version = 1L;
        long instant = 1000L;

        log.setOrUpdateLastCommit(entryId, version, instant);
        log.removeEntry(entryId, version);

        assertThrows(NullPointerException.class, () -> {
            log.getLastCommit(entryId, version);
        }, "An exception should be thrown after removing the entry.");
    }

    @Test
    void testRemoveEntryForNonexistentEntry() {
        long entryId = 1L;
        long version = 1L;

        assertDoesNotThrow(() -> log.removeEntry(entryId, version),
                "Removing a nonexistent entry should not throw an exception.");
    }

    @Test
    void testMultipleEntries() {
        long entryId1 = 1L;
        long version1 = 1L;
        long instant1 = 1000L;

        long entryId2 = 2L;
        long version2 = 2L;
        long instant2 = 2000L;

        log.setOrUpdateLastCommit(entryId1, version1, instant1);
        log.setOrUpdateLastCommit(entryId2, version2, instant2);

        assertEquals(instant1, log.getLastCommit(entryId1, version1),
                "The first entry's instant should match its updated value.");
        assertEquals(instant2, log.getLastCommit(entryId2, version2),
                "The second entry's instant should match its updated value.");
    }

}

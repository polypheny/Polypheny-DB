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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class IdentifierIntervalTest {

    @Test
    void testCreatingIntervalLowerBound() {
        IdentifierInterval interval = new IdentifierInterval(10, 20);
        assertEquals(10, interval.getLowerBound());
    }

    @Test
    void testCreatingIntervalUpperBound() {
        IdentifierInterval interval = new IdentifierInterval(10, 20);
        assertEquals(20, interval.getUpperBound());
    }

    @Test
    void testCreatingIntervalInvalidBounds() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new IdentifierInterval(20, 10);
        });
        assertTrue(exception.getMessage().contains("Upper bound must be greater or equal than lower bound"));
    }

    @Test
    void testHasNextIdentifier() {
        IdentifierInterval interval = new IdentifierInterval(10, 20);
        assertTrue(interval.hasNextIdentifier());
    }

    @Test
    void testRetrievingIdentifiersUntilEmpty() {
        IdentifierInterval interval = new IdentifierInterval(10, 12);
        assertEquals(10, interval.getNextIdentifier());
        assertEquals(11, interval.getNextIdentifier());
        assertFalse(interval.hasNextIdentifier());
    }

    @Test
    void testIntervalComparisonLess() {
        IdentifierInterval interval1 = new IdentifierInterval(5, 10);
        IdentifierInterval interval2 = new IdentifierInterval(10, 15);
        assertTrue(interval1.compareTo(interval2) < 0, "Interval1 should be less than Interval2");
    }

    @Test
    void testIntervalComparisonGreater() {
        IdentifierInterval interval1 = new IdentifierInterval(5, 10);
        IdentifierInterval interval2 = new IdentifierInterval(1, 5);
        assertTrue(interval1.compareTo(interval2) > 0, "Interval1 should be greater than Interval2");
    }

    @Test
    void testIntervalComparisonEquals() {
        IdentifierInterval interval1 = new IdentifierInterval(5, 10);
        IdentifierInterval interval2 = new IdentifierInterval(5, 10);
        assertEquals(0, interval1.compareTo(interval2));
    }

    @Test
    void testIntervalComparisonIncludes() {
        IdentifierInterval interval1 = new IdentifierInterval(5, 10);
        IdentifierInterval interval2 = new IdentifierInterval(1, 20);
        assertTrue( interval1.compareTo( interval2 ) > 0, "Interval1 should be greater than Interval2");
    }

    @Test
    void testIntervalToString() {
        IdentifierInterval interval1 = new IdentifierInterval(5, 10);
        assertEquals("IdentifierInterval{5, 10}", interval1.toString());
    }

}

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

package org.polypheny.db.cql.utils;

import org.junit.Assert;
import org.junit.Test;


public class ExclusiveComparisonsTest {

    private final boolean[] exclusivelyTrue = new boolean[]{
            false,
            true,
            false,
            false,
            false
    };

    private final boolean[] notExclusivelyTrue = new boolean[]{
            true,
            false,
            true,
            true,
            false
    };

    private final boolean[] allFalse = new boolean[]{
            false,
            false,
            false,
            false,
            false
    };

    private final boolean[] allTrue = new boolean[]{
            true,
            true,
            true,
            true,
            true
    };


    @Test
    public void testIsExclusivelyTrue() {
        Assert.assertTrue( ExclusiveComparisons.IsExclusivelyTrue( false, exclusivelyTrue ) );
        Assert.assertFalse( ExclusiveComparisons.IsExclusivelyTrue( true, exclusivelyTrue ) );
        Assert.assertFalse( ExclusiveComparisons.IsExclusivelyTrue( false, notExclusivelyTrue ) );
        Assert.assertFalse( ExclusiveComparisons.IsExclusivelyTrue( true, notExclusivelyTrue ) );
        Assert.assertFalse( ExclusiveComparisons.IsExclusivelyTrue( false, allTrue ) );
        Assert.assertFalse( ExclusiveComparisons.IsExclusivelyTrue( true, allTrue ) );
        Assert.assertFalse( ExclusiveComparisons.IsExclusivelyTrue( false, allFalse ) );
        Assert.assertTrue( ExclusiveComparisons.IsExclusivelyTrue( true, allFalse ) );
    }


    @Test
    public void testGetExclusivelyTrue() {
        Assert.assertEquals( 2, ExclusiveComparisons.GetExclusivelyTrue( false, exclusivelyTrue ) );
        Assert.assertEquals( -1, ExclusiveComparisons.GetExclusivelyTrue( true, exclusivelyTrue ) );
        Assert.assertEquals( -1, ExclusiveComparisons.GetExclusivelyTrue( false, notExclusivelyTrue ) );
        Assert.assertEquals( -1, ExclusiveComparisons.GetExclusivelyTrue( true, notExclusivelyTrue ) );
        Assert.assertEquals( -1, ExclusiveComparisons.GetExclusivelyTrue( false, allTrue ) );
        Assert.assertEquals( -1, ExclusiveComparisons.GetExclusivelyTrue( true, allTrue ) );
        Assert.assertEquals( -1, ExclusiveComparisons.GetExclusivelyTrue( false, allFalse ) );
        Assert.assertEquals( 0, ExclusiveComparisons.GetExclusivelyTrue( true, allFalse ) );
    }

}

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

package org.polypheny.db.adapter.cottontail.algebra;


import org.junit.Assert;
import org.junit.Test;
import org.polypheny.db.adapter.cottontail.algebra.CottontailFilter.AtomicPredicate;
import org.polypheny.db.adapter.cottontail.algebra.CottontailFilter.BooleanPredicate;
import org.polypheny.db.adapter.cottontail.algebra.CottontailFilter.CompoundPredicate;
import org.polypheny.db.adapter.cottontail.algebra.CottontailFilter.CompoundPredicate.Op;


public class CottontailFilterTest {

    @Test
    public void simpleCnfTest() {
        BooleanPredicate testPredicate = new CompoundPredicate( Op.ROOT,
                new CompoundPredicate( Op.NOT,
                        new CompoundPredicate(
                                Op.OR,
                                new AtomicPredicate( null, false ),
                                new AtomicPredicate( null, false )
                        ), null ), null );

        while ( testPredicate.simplify() )
            ;

        CompoundPredicate result = (CompoundPredicate) ((CompoundPredicate) testPredicate).left;

        Assert.assertEquals( "Highest up predicate should be AND.", Op.AND, result.op );
        Assert.assertEquals( "Inner operations should be negation", Op.NOT, ((CompoundPredicate) result.left).op );
        Assert.assertEquals( "Inner operations should be negation", Op.NOT, ((CompoundPredicate) result.right).op );
    }

}

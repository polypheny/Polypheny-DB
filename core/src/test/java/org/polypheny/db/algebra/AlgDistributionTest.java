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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.algebra;


import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.polypheny.db.plan.AlgTraitSet;


/**
 * Tests for {@link AlgDistribution}.
 */
public class AlgDistributionTest {

    @Test
    public void testRelDistributionSatisfy() {
        AlgDistribution distribution1 = AlgDistributions.hash( ImmutableList.of( 0 ) );
        AlgDistribution distribution2 = AlgDistributions.hash( ImmutableList.of( 1 ) );

        AlgTraitSet traitSet = AlgTraitSet.createEmpty();
        AlgTraitSet simpleTrait1 = traitSet.plus( distribution1 );
        AlgTraitSet simpleTrait2 = traitSet.plus( distribution2 );
        AlgTraitSet compositeTrait = traitSet.replace( AlgDistributionTraitDef.INSTANCE, ImmutableList.of( distribution1, distribution2 ) );

        assertThat( compositeTrait.satisfies( simpleTrait1 ), is( true ) );
        assertThat( compositeTrait.satisfies( simpleTrait2 ), is( true ) );

        assertThat( distribution1.compareTo( distribution2 ), is( -1 ) );
        assertThat( distribution2.compareTo( distribution1 ), is( 1 ) );
        //noinspection EqualsWithItself
        assertThat( distribution2.compareTo( distribution2 ), is( 0 ) );
    }

}

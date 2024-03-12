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

package org.polypheny.db.plan;


import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgCollations;


/**
 * Test to verify {@link AlgCompositeTrait}.
 */
public class AlgTraitTest {

    private static final AlgCollationTraitDef COLLATION = AlgCollationTraitDef.INSTANCE;


    private void assertCanonical( String message, Supplier<List<AlgCollation>> collation ) {
        AlgTrait<?> trait1 = AlgCompositeTrait.of( COLLATION, collation.get() );
        AlgTrait<?> trait2 = AlgCompositeTrait.of( COLLATION, collation.get() );

        assertEquals(
                trait1 + " @" + Integer.toHexString( System.identityHashCode( trait1 ) ),
                trait2 + " @" + Integer.toHexString( System.identityHashCode( trait2 ) ),
                "AlgCompositeTrait.of should return the same instance for " + message );
    }


    @Test
    public void compositeEmpty() {
        assertCanonical( "empty composite", ImmutableList::of );
    }


    @Test
    public void compositeOne() {
        assertCanonical( "composite with one element", () -> ImmutableList.of( AlgCollations.of( ImmutableList.of() ) ) );
    }


    @Test
    public void compositeTwo() {
        assertCanonical( "composite with two elements", () -> ImmutableList.of( AlgCollations.of( 0 ), AlgCollations.of( 1 ) ) );
    }

}


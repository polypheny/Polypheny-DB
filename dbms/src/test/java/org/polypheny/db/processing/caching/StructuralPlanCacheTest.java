/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.processing.caching;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.PlanKey;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.prepare.Prepare.PreparedResult;
import org.polypheny.db.processing.caching.ImplementationCache.CachedImplementation;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.type.PolyType;

@Tag("adapter")
class StructuralPlanCacheTest {

    private final AlgDataTypeFactory types = AlgDataTypeFactory.DEFAULT;
    private final RexBuilder rex = new RexBuilder( types );
    private final QueryPlanCache plans = new QueryPlanCache();
    private final ImplementationCache implementations = new ImplementationCache();


    @BeforeAll
    static void startDatabase() {
        TestHelper.getInstance();
    }


    @Test
    void bothCachesSeparateCollidingLiteralValues() {
        assertEquals( "Aa".hashCode(), "BB".hashCode() );
        PlanKey firstKey = PlanKey.of( rex.makeLiteral( "Aa" ) ).orElseThrow();
        PlanKey secondKey = PlanKey.of( rex.makeLiteral( "BB" ) ).orElseThrow();
        CachedImplementation first = cachedImplementation();
        CachedImplementation second = cachedImplementation();

        plans.put( firstKey, first.optimalNode() );
        implementations.put( firstKey, first );
        assertNull( plans.getIfPresent( secondKey ) );
        assertNull( implementations.getIfPresent( secondKey ) );

        plans.put( secondKey, second.optimalNode() );
        implementations.put( secondKey, second );
        assertSame( first.optimalNode(), plans.getIfPresent( firstKey ) );
        assertSame( second.optimalNode(), plans.getIfPresent( secondKey ) );
        assertSame( first, implementations.getIfPresent( firstKey ) );
        assertSame( second, implementations.getIfPresent( secondKey ) );
    }


    @Test
    void bothCachesReuseEquivalentRuntimeParameterSlots() {
        AlgDataType parameterType = types.createPolyType( PolyType.INTEGER );
        PlanKey firstExecution = PlanKey.of( rex.makeDynamicParam( parameterType, 0 ) ).orElseThrow();
        PlanKey nextExecution = PlanKey.of( new RexBuilder( types ).makeDynamicParam( parameterType, 0 ) ).orElseThrow();
        CachedImplementation first = cachedImplementation();

        plans.put( firstExecution, first.optimalNode() );
        implementations.put( firstExecution, first );

        assertSame( first.optimalNode(), plans.getIfPresent( nextExecution ) );
        assertSame( first, implementations.getIfPresent( nextExecution ) );
    }


    @SuppressWarnings("unchecked")
    private static CachedImplementation cachedImplementation() {
        return new CachedImplementation( mock( AlgNode.class ), mock( PreparedResult.class ) );
    }

}

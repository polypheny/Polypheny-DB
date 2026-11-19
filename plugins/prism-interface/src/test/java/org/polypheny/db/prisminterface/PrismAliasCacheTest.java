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

package org.polypheny.db.prisminterface;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.polypheny.db.prisminterface.CypherTest.relationalFrame;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.prisminterface.CypherTest.PrismConnection;
import org.polypheny.db.processing.caching.ImplementationCache;
import org.polypheny.db.processing.caching.QueryPlanCache;
import org.polypheny.prism.ColumnMeta;
import org.polypheny.prism.Frame;
import org.polypheny.prism.RelationalFrame;

@Tag("adapter")
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class PrismAliasCacheTest {

    @BeforeAll
    static void startDatabase() {
        TestHelper.getInstance();
    }


    @Test
    void preservesDuplicateAliasMetadataOnAnImplementationCacheHit() throws IOException {
        boolean planCaching = RuntimeConfig.QUERY_PLAN_CACHING.getBoolean();
        boolean implementationCaching = RuntimeConfig.IMPLEMENTATION_CACHING.getBoolean();
        RuntimeConfig.QUERY_PLAN_CACHING.setBoolean( true );
        RuntimeConfig.IMPLEMENTATION_CACHING.setBoolean( true );
        QueryPlanCache.INSTANCE.reset();

        String sql = """
                SELECT id AS same, id + 1 AS same
                FROM (VALUES (7), (8)) AS fixture(id)
                WHERE id = 9
                """;
        String namespace = Catalog.DEFAULT_NAMESPACE_NAME;
        List<String> aliases = List.of( "same", "same0" );
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame firstFrame = connection.execute( namespace, "sql", sql, -1 ).getResult().getFrame();
            RelationalFrame first = relationalFrame( firstFrame, true, aliases );
            assertEquals( 0, first.getRowsCount() );
            assertEquals( aliases, first.getColumnMetaList().stream().map( ColumnMeta::getColumnName ).toList() );
            long hits = ImplementationCache.INSTANCE.getHitCount();

            Frame secondFrame = connection.execute( namespace, "sql", sql, -1 ).getResult().getFrame();
            RelationalFrame second = relationalFrame( secondFrame, true, aliases );
            assertTrue( ImplementationCache.INSTANCE.getHitCount() > hits, "Expected the second execution to reuse the cached implementation" );
            assertEquals( first, second );
        } finally {
            QueryPlanCache.INSTANCE.reset();
            RuntimeConfig.QUERY_PLAN_CACHING.setBoolean( planCaching );
            RuntimeConfig.IMPLEMENTATION_CACHING.setBoolean( implementationCaching );
        }
    }

}

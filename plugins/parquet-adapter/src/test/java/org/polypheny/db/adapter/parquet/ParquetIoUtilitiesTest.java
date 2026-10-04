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

package org.polypheny.db.adapter.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.parquet.shared.io.ParquetCancellation;
import org.polypheny.db.adapter.parquet.shared.io.ParquetUrlResolver;

class ParquetIoUtilitiesTest {

    @Test
    void cancellationChecksOnlyAtConfiguredRowIntervals() {
        AtomicBoolean cancelled = new AtomicBoolean( true );

        assertTrue( ParquetCancellation.shouldStop( 0, cancelled ) );
        assertFalse( ParquetCancellation.shouldStop( 1, cancelled ) );
        assertFalse( ParquetCancellation.shouldStop( 4095, cancelled ) );
        assertTrue( ParquetCancellation.shouldStop( 4096, cancelled ) );

        cancelled.set( false );
        assertFalse( ParquetCancellation.shouldStop( 4096, cancelled ) );
    }


    @Test
    void urlResolverNormalizesDirectoriesAndKeepsExplicitParquetFiles() throws Exception {
        URL directory = new URL( "file:/tmp/parquet-data" );
        URL directoryWithSlash = new URL( "file:/tmp/parquet-data/" );
        URL sourceFile = new URL( "file:/tmp/parquet-data/orders.parquet" );

        assertNull( ParquetUrlResolver.asDirectoryUrl( null ) );
        assertEquals( directoryWithSlash, ParquetUrlResolver.asDirectoryUrl( directory ) );
        assertSame( directoryWithSlash, ParquetUrlResolver.asDirectoryUrl( directoryWithSlash ) );
        assertEquals( directoryWithSlash, ParquetUrlResolver.asSourceUrl( directory ) );
        assertSame( sourceFile, ParquetUrlResolver.asSourceUrl( sourceFile ) );
        assertEquals( new URL( "file:/tmp/parquet-data/part-000.parquet" ), ParquetUrlResolver.resolveFile( directory, "part-000.parquet" ) );
        assertSame( sourceFile, ParquetUrlResolver.resolveFile( sourceFile, "ignored.parquet" ) );
    }

}

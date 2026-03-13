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

package org.polypheny.db.adapter.parquet.io;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.Sources;

/**
 * Utility methods for discovering Parquet files.
 */
public class ParquetFileDiscovery {

    private ParquetFileDiscovery() {
    }


    /**
     * Lists Parquet files below the configured source location.
     */
    public static Set<String> listParquetFiles( URL baseDir ) {
        if ( Sources.of( baseDir ).file().isFile() ) {
            return Set.of( Sources.of( baseDir ).file().getName() );
        }

        File[] files = Sources.of( baseDir ).file().listFiles( ( d, name ) -> isParquetFile( name ) );
        if ( files == null ) {
            throw new GenericRuntimeException( "No .parquet files were found." );
        }
        return Arrays.stream( files ).map( File::getName ).collect( Collectors.toSet() );
    }


    /**
     * Checks whether the file name looks like a Parquet file.
     */
    public static boolean isParquetFile( String fileName ) {
        return fileName != null && fileName.toLowerCase().endsWith( ".parquet" );
    }

}

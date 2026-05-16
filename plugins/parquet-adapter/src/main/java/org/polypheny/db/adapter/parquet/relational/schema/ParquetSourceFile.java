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

package org.polypheny.db.adapter.parquet.relational.schema;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import org.polypheny.db.adapter.parquet.shared.statistics.ParquetColumnStatisticsReader;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.Source;
import org.polypheny.db.util.Sources;


/**
 * One physical Parquet file that contributes rows to a Parquet table.
 */
public record ParquetSourceFile(
        String fileUrl,
        Map<String, String> partitionValues,
        Map<List<String>, ParquetColumnStatistics> columnStatistics ) {

    public ParquetSourceFile {
        partitionValues = partitionValues == null ? Map.of() : Collections.unmodifiableMap( new LinkedHashMap<>( partitionValues ) );
        columnStatistics = immutableColumnStatistics( columnStatistics );
    }


    public static ParquetSourceFile of( String fileUrl ) {
        return of( fileUrl, Map.of() );
    }


    public static ParquetSourceFile of( String fileUrl, Map<String, String> partitionValues ) {
        return new ParquetSourceFile( fileUrl, partitionValues, ParquetColumnStatisticsReader.readAll( source( fileUrl ) ) );
    }


    public Source asSource() {
        return source( fileUrl );
    }


    private static Source source( String fileUrl ) {
        try {
            return Sources.of( new URL( fileUrl ) );
        } catch ( MalformedURLException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    private static Map<List<String>, ParquetColumnStatistics> immutableColumnStatistics( Map<List<String>, ParquetColumnStatistics> columnStatistics ) {
        if ( columnStatistics == null || columnStatistics.isEmpty() ) {
            return Map.of();
        }
        Map<List<String>, ParquetColumnStatistics> copy = new LinkedHashMap<>();
        columnStatistics.forEach( ( path, statistics ) -> copy.put( List.copyOf( path ), statistics ) );
        return Collections.unmodifiableMap( copy );
    }

}

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
import java.util.LinkedHashMap;
import java.util.Map;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.Source;
import org.polypheny.db.util.Sources;


/**
 * One physical Parquet file that contributes rows to a Parquet table.
 */
public record ParquetSourceFile(
        String fileUrl,
        Map<String, String> partitionValues ) {

    public ParquetSourceFile {
        partitionValues = partitionValues == null ? Map.of() : Collections.unmodifiableMap( new LinkedHashMap<>( partitionValues ) );
    }


    public static ParquetSourceFile of( String fileUrl ) {
        return new ParquetSourceFile( fileUrl, Map.of() );
    }


    public Source asSource() {
        try {
            return Sources.of( new URL( fileUrl ) );
        } catch ( MalformedURLException e ) {
            throw new GenericRuntimeException( e );
        }
    }

}

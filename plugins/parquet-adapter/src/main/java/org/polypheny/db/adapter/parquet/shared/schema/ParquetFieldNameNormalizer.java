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

package org.polypheny.db.adapter.parquet.shared.schema;

import org.polypheny.db.adapter.parquet.shared.schema.inference.FieldSchema;
import org.polypheny.db.adapter.parquet.shared.schema.inference.ValueKind;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Shared Parquet field-name normalization used across Parquet consumers.
 */
public final class ParquetFieldNameNormalizer {

    private ParquetFieldNameNormalizer() {
    }


    public static String normalizeFieldName( String name ) {
        return name.toLowerCase( Locale.ROOT ).trim().replaceAll( "[^a-z0-9_]+", "_" );
    }


    /**
     * Normalize and uniquify Parquet names:
     * convert source names into valid Parquet-safe names and resolves duplicates
     *
     * @param fields - schema fields
     */
    public static void uniquifyParquetNames( List<FieldSchema> fields ) {
        Set<String> seen = new LinkedHashSet<>();
        for ( FieldSchema field : fields ) {
            String baseName = normalizeFieldName( field.getSourceName() );
            if ( baseName.isBlank() ) {
                baseName = "field";
            }
            String candidate = baseName;
            int suffix = 2;
            while ( seen.contains( candidate ) ) {
                candidate = baseName + "_" + suffix++;
            }
            field.setParquetName( candidate );
            seen.add( candidate );
            if ( field.getValueSchema().kind() == ValueKind.GROUP ) {
                uniquifyParquetNames( field.getValueSchema().nested() );
            }
        }
    }

}

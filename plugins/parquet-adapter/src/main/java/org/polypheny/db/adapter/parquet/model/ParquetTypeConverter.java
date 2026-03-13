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

package org.polypheny.db.adapter.parquet.model;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.type.PolyType;

public class ParquetTypeConverter {

    /**
     * Convert original parquet type into {@link PolyType}
     * @param field parquet type
     * @return {@link PolyType}
     */
    public PolyType fromParquetTypeToPolyType( Type field ) {

        // treat nested types as string
        if ( !field.isPrimitive() ) {
            return PolyType.VARCHAR;
        }

        // not nested types
        PrimitiveType primitive = field.asPrimitiveType();
        LogicalTypeAnnotation logical = primitive.getLogicalTypeAnnotation();

        // date types
        if ( logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation ) {
            return PolyType.DATE;
        }
        if ( logical instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation ) {
            return PolyType.TIME;
        }
        if ( logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ) {
            return PolyType.TIMESTAMP;
        }

        // string like types
        if ( logical instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation
                || logical instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation
                || logical instanceof LogicalTypeAnnotation.JsonLogicalTypeAnnotation ) {
            return PolyType.VARCHAR;
        }

        // primitive types
        return switch ( primitive.getPrimitiveTypeName() ) {
            case BOOLEAN -> PolyType.BOOLEAN;
            case INT32 -> PolyType.INTEGER;
            case INT64 -> PolyType.BIGINT;
            case FLOAT -> PolyType.REAL;
            case DOUBLE -> PolyType.DOUBLE;
            case FIXED_LEN_BYTE_ARRAY, BINARY, INT96 -> PolyType.VARBINARY;
        };
    }
}

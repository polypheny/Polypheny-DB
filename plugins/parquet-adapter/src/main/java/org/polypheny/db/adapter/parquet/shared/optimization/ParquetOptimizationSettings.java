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

package org.polypheny.db.adapter.parquet.shared.optimization;

/**
 * Shared switches for Parquet planner optimizations.
 */
public final class ParquetOptimizationSettings {

    public static final String OPTIMIZE_AGGREGATION_PROPERTY = "polypheny.parquet.optimizeAggregation";
    public static final String OPTIMIZE_AGGREGATION_ENV = "POLYPHENY_PARQUET_OPTIMIZE_AGGREGATION";


    private ParquetOptimizationSettings() {
    }


    public static boolean isOptimizeAggregation() {
        String property = System.getProperty( OPTIMIZE_AGGREGATION_PROPERTY );
        if ( property != null ) {
            return parseBoolean( property, true );
        }
        return parseBoolean( System.getenv( OPTIMIZE_AGGREGATION_ENV ), true );
    }


    @SuppressWarnings("SameParameterValue")
    private static boolean parseBoolean( String value, boolean fallback ) {
        if ( value == null || value.isBlank() ) {
            return fallback;
        }
        return switch ( value.trim().toLowerCase() ) {
            case "true", "1", "yes", "y", "on" -> true;
            case "false", "0", "no", "n", "off" -> false;
            default -> fallback;
        };
    }

}

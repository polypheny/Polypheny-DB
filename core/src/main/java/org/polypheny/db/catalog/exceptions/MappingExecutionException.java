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

package org.polypheny.db.catalog.exceptions;

import org.polypheny.db.type.ScalarConversions;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Describes a failed mapped value consistently
 */
public final class MappingExecutionException extends MappingException {

    private MappingExecutionException( String mappingName, String targetTableName, String column, String problem, Throwable cause ) {
        super( "Mapping '%s', target '%s', column '%s': %s".formatted( mappingName, targetTableName, column, problem ), cause );
    }


    public static MappingExecutionException conversionFailed( String mappingName, String targetTableName, String column, PolyValue value,
            String sourceType, String targetType, Throwable cause ) {
        return new MappingExecutionException( mappingName, targetTableName, column,
                "cannot convert value %s from %s to %s.".formatted( displayValue( value ), sourceType, targetType ), cause );
    }


    public static MappingExecutionException nullNotAllowed( String mappingName, String targetTableName, String column, String sourceType, String targetType ) {
        return new MappingExecutionException( mappingName, targetTableName, column,
                "cannot map value NULL from %s to %s because the target column does not allow NULL.".formatted( sourceType, targetType ), null );
    }


    private static String displayValue( PolyValue value ) {
        if ( ScalarConversions.isNull( value ) ) {
            return "NULL";
        }
        String text = value.isString() ? value.asString().toQuotedJson() : value.toJson();
        return text != null && text.length() > 160 ? text.substring( 0, 160 ) + "…" : text;
    }
}

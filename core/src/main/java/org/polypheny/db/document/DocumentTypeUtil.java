/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.document;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import org.polypheny.db.rex.RexLiteral;

public class DocumentTypeUtil {


    public static boolean validateJson( String json ) {
        try {
            // maybe use same Json library for everything TODO DL
            final ObjectMapper mapper = new ObjectMapper();
            mapper.readTree( json );
            return true;
        } catch ( JsonProcessingException e ) {
            return false;
        }

    }


    public static Comparable<?> getMqlType( RexLiteral literal ) {
        switch ( literal.getTypeName() ) {

            case BOOLEAN:
                return literal.getValueAs( Boolean.class );
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return literal.getValueAs( Integer.class );
            case BIGINT:
                return literal.getValueAs( Long.class );
            case DECIMAL:
                return literal.getValueAs( BigDecimal.class );
            case FLOAT:
            case REAL:
                return literal.getValueAs( Float.class );
            case DOUBLE:
                return literal.getValueAs( Double.class );
            case DATE:
                break;
            case TIME:
                break;
            case TIME_WITH_LOCAL_TIME_ZONE:
                break;
            case TIMESTAMP:
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                break;
            case INTERVAL_YEAR:
                break;
            case INTERVAL_YEAR_MONTH:
                break;
            case INTERVAL_MONTH:
                break;
            case INTERVAL_DAY:
                break;
            case INTERVAL_DAY_HOUR:
                break;
            case INTERVAL_DAY_MINUTE:
                break;
            case INTERVAL_DAY_SECOND:
                break;
            case INTERVAL_HOUR:
                break;
            case INTERVAL_HOUR_MINUTE:
                break;
            case INTERVAL_HOUR_SECOND:
                break;
            case INTERVAL_MINUTE:
                break;
            case INTERVAL_MINUTE_SECOND:
                break;
            case INTERVAL_SECOND:
                break;
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case JSON:
                return literal.getValueAs( String.class );
            case NULL:
                break;
            case ANY:
                break;
            case SYMBOL:
                break;
            case MULTISET:
                break;
            case ARRAY:
                break;
            case MAP:
                break;
            case DISTINCT:
                break;
            case STRUCTURED:
                break;
            case ROW:
                break;
            case OTHER:
                break;
            case CURSOR:
                break;
            case COLUMN_LIST:
                break;
            case DYNAMIC_STAR:
                break;
            case GEOMETRY:
                break;
            case FILE:
                break;
            case IMAGE:
                break;
            case VIDEO:
                break;
            case SOUND:
                break;
        }
        throw new RuntimeException( "This type cannot be translated yet." );
    }

}

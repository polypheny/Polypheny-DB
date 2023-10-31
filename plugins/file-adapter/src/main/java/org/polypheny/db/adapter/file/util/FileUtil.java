/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.adapter.file.util;

import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.type.entity.PolyValue;

public class FileUtil {

    public static Object toObject( PolyValue value ) {
        return value == null ? null : value.toJson();
    }


    public static Object fromValue( PolyValue value ) {
        if ( value == null ) {
            return null;
        }
        switch ( value.type ) {
            case INTEGER:
            case TINYINT:
            case SMALLINT:
                return value.asNumber().IntValue();
            case REAL:
            case FLOAT:
                return value.asNumber().FloatValue();
            case VARCHAR:
            case CHAR:
                return value.asString().value;
            case BIGINT:
            case DECIMAL:
                return value.asNumber().BigDecimalValue();
            case BINARY:
            case VARBINARY:
                return value.asBinary().value.getBytes();
            case DOUBLE:
                return value.asNumber().DoubleValue();
            case BOOLEAN:
                return value.asBoolean().value;
            case DATE:
                return value.asDate().getDaysSinceEpoch();
            case TIME:
                return value.asTime().ofDay;
            case TIMESTAMP:
                return value.asTimeStamp().milliSinceEpoch;
        }
        throw new NotImplementedException();
    }

}

/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.sql.language;


import com.google.common.base.Preconditions;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.TimestampString;


/**
 * A SQL literal representing a TIMESTAMP value, for example <code>TIMESTAMP '1969-07-21 03:15 GMT'</code>.
 *
 * Create values using {@link SqlLiteral#createTimestamp}.
 */
public class SqlTimestampLiteral extends SqlAbstractDateTimeLiteral {


    SqlTimestampLiteral( PolyTimestamp ts, int precision, boolean hasTimeZone, ParserPos pos ) {
        super( ts, hasTimeZone, PolyType.TIMESTAMP, precision, pos );
        Preconditions.checkArgument( this.precision >= 0 );
    }


    @Override
    public SqlTimestampLiteral clone( ParserPos pos ) {
        return new SqlTimestampLiteral( (PolyTimestamp) value, precision, hasTimeZone, pos );
    }


    public String toString() {
        return "TIMESTAMP '" + toFormattedString() + "'";
    }


    /**
     * Returns e.g. '03:05:67.456'.
     */
    @Override
    public String toFormattedString() {
        TimestampString ts = getTimestamp();
        if ( precision > 0 ) {
            ts = ts.round( precision );
        }
        return ts.toString( precision );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.getDialect().unparseDateTimeLiteral( writer, this, leftPrec, rightPrec );
    }

}


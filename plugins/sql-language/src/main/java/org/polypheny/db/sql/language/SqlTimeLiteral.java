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
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.util.TimeString;


/**
 * A SQL literal representing a TIME value, for example <code>TIME '14:33:44.567'</code>.
 *
 * Create values using {@link SqlLiteral#createTime}.
 */
public class SqlTimeLiteral extends SqlAbstractDateTimeLiteral {


    SqlTimeLiteral( PolyTime t, int precision, boolean hasTimeZone, ParserPos pos ) {
        super( t, hasTimeZone, PolyType.TIME, precision, pos );
        Preconditions.checkArgument( this.precision >= 0 );
    }


    /**
     * Converts this literal to a {@link TimeString}.
     */
    protected TimeString getTime() {
        return TimeString.fromMillisOfDay( value.asTime().getOfDay() );
    }


    @Override
    public SqlTimeLiteral clone( ParserPos pos ) {
        return new SqlTimeLiteral( (PolyTime) value, precision, hasTimeZone, pos );
    }


    public String toString() {
        return "TIME '" + toFormattedString() + "'";
    }


    /**
     * Returns e.g. '03:05:67.456'.
     */
    @Override
    public String toFormattedString() {
        return getTime().toString( precision );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.getDialect().unparseDateTimeLiteral( writer, this, leftPrec, rightPrec );
    }

}


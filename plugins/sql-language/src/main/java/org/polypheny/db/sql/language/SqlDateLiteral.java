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


import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.util.DateString;


/**
 * A SQL literal representing a DATE value, such as <code>DATE '2004-10-22'</code>.
 *
 * Create values using {@link SqlLiteral#createDate}.
 */
public class SqlDateLiteral extends SqlAbstractDateTimeLiteral {


    SqlDateLiteral( PolyDate d, ParserPos pos ) {
        super( d, false, PolyType.DATE, 0, pos );
    }


    /**
     * Converts this literal to a {@link DateString}.
     */
    protected DateString getDate() {
        return DateString.fromCalendarFields( value.asTemporal().toCalendar() );
    }


    @Override
    public SqlDateLiteral clone( ParserPos pos ) {
        return new SqlDateLiteral( (PolyDate) value, pos );
    }


    @Override
    public String toString() {
        return "DATE '" + toFormattedString() + "'";
    }


    /**
     * Returns e.g. '1969-07-21'.
     */
    @Override
    public String toFormattedString() {
        return getDate().toString();
    }


    @Override
    public AlgDataType createSqlType( AlgDataTypeFactory typeFactory ) {
        return typeFactory.createPolyType( getTypeName() );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.getDialect().unparseDateTimeLiteral( writer, this, leftPrec, rightPrec );
    }

}


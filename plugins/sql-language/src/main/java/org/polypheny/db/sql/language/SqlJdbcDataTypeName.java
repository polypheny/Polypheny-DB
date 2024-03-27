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

package org.polypheny.db.sql.language;


import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.TimeUnitRange;
import org.polypheny.db.sql.language.fun.SqlConvertFunction;
import org.polypheny.db.type.PolyType;


/**
 * Defines the name of the types which can occur as a type argument in a JDBC <code>{fn CONVERT(value, type)}</code> function.
 * (This function has similar functionality to {@code CAST}, and is not to be confused with the SQL standard {@link SqlConvertFunction CONVERT} function.)
 *
 * @see SqlJdbcFunctionCall
 */
public enum SqlJdbcDataTypeName {
    SQL_CHAR( PolyType.CHAR ),
    SQL_VARCHAR( PolyType.VARCHAR ),
    SQL_DATE( PolyType.DATE ),
    SQL_TIME( PolyType.TIME ),
    SQL_TIMESTAMP( PolyType.TIMESTAMP ),
    SQL_DECIMAL( PolyType.DECIMAL ),
    SQL_NUMERIC( PolyType.DECIMAL ),
    SQL_BOOLEAN( PolyType.BOOLEAN ),
    SQL_INTEGER( PolyType.INTEGER ),
    SQL_BINARY( PolyType.BINARY ),
    SQL_VARBINARY( PolyType.VARBINARY ),
    SQL_TINYINT( PolyType.TINYINT ),
    SQL_SMALLINT( PolyType.SMALLINT ),
    SQL_BIGINT( PolyType.BIGINT ),
    SQL_REAL( PolyType.REAL ),
    SQL_DOUBLE( PolyType.DOUBLE ),
    SQL_FLOAT( PolyType.FLOAT ),
    SQL_INTERVAL_YEAR( TimeUnitRange.YEAR ),
    SQL_INTERVAL_YEAR_TO_MONTH( TimeUnitRange.YEAR_TO_MONTH ),
    SQL_INTERVAL_MONTH( TimeUnitRange.MONTH ),
    SQL_INTERVAL_DAY( TimeUnitRange.DAY ),
    SQL_INTERVAL_DAY_TO_HOUR( TimeUnitRange.DAY_TO_HOUR ),
    SQL_INTERVAL_DAY_TO_MINUTE( TimeUnitRange.DAY_TO_MINUTE ),
    SQL_INTERVAL_DAY_TO_SECOND( TimeUnitRange.DAY_TO_SECOND ),
    SQL_INTERVAL_HOUR( TimeUnitRange.HOUR ),
    SQL_INTERVAL_HOUR_TO_MINUTE( TimeUnitRange.HOUR_TO_MINUTE ),
    SQL_INTERVAL_HOUR_TO_SECOND( TimeUnitRange.HOUR_TO_SECOND ),
    SQL_INTERVAL_MINUTE( TimeUnitRange.MINUTE ),
    SQL_INTERVAL_MINUTE_TO_SECOND( TimeUnitRange.MINUTE_TO_SECOND ),
    SQL_INTERVAL_SECOND( TimeUnitRange.SECOND ),
    SQL_JSON( PolyType.JSON );

    private final TimeUnitRange range;
    private final PolyType typeName;


    SqlJdbcDataTypeName( PolyType typeName ) {
        this( typeName, null );
    }


    SqlJdbcDataTypeName( TimeUnitRange range ) {
        this( null, range );
    }


    SqlJdbcDataTypeName( PolyType typeName, TimeUnitRange range ) {
        assert (typeName == null) != (range == null);
        this.typeName = typeName;
        this.range = range;
    }


    /**
     * Creates a parse-tree node representing an occurrence of this keyword at a particular position in the parsed text.
     */
    public SqlLiteral symbol( ParserPos pos ) {
        return SqlLiteral.createSymbol( this, pos );
    }


    /**
     * Creates a parse tree node for a type identifier of this name.
     */
    public SqlNode createDataType( ParserPos pos ) {
        if ( typeName != null ) {
            assert range == null;
            final SqlIdentifier id = new SqlIdentifier( typeName.name(), pos );
            return new SqlDataTypeSpec( id, -1, -1, null, null, pos );
        } else {
            assert range != null;
            return new SqlIntervalQualifier( range.startUnit, range.endUnit, pos );
        }
    }
}


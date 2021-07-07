/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.sql.dialect;


import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.SqlCall;
import org.polypheny.db.sql.SqlDataTypeSpec;
import org.polypheny.db.sql.SqlDialect;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.parser.SqlParserPos;


/**
 * A <code>SqlDialect</code> implementation for the MonetDB database.
 */
@Slf4j
public class MonetdbSqlDialect extends SqlDialect {

    public static final SqlDialect DEFAULT = new MonetdbSqlDialect(
            EMPTY_CONTEXT.withDatabaseProduct( DatabaseProduct.MONETDB ).withIdentifierQuoteString( "\"" )
    );


    /**
     * Creates an MonetdbSqlDialect.
     */
    public MonetdbSqlDialect( Context context ) {
        super( context );
    }


    @Override
    public boolean supportsCharSet() {
        return false;
    }


    @Override
    public boolean supportsColumnNamesWithSchema() {
        return false;
    }


    @Override
    public IntervalParameterStrategy getIntervalParameterStrategy() {
        return IntervalParameterStrategy.MULTIPLICATION;
    }


    @Override
    public SqlNode getCastSpec( RelDataType type ) {
        String castSpec;
        switch ( type.getPolyType() ) {
            case ARRAY:
                // We need to flag the type with a underscore to flag the type (the underscore is removed in the unparse method)
                castSpec = "_TEXT";
                break;
            case FILE:
            case IMAGE:
            case VIDEO:
            case SOUND:
                // We need to flag the type with a underscore to flag the type (the underscore is removed in the unparse method)
                castSpec = "_BLOB";
                break;
            default:
                return super.getCastSpec( type );
        }

        return new SqlDataTypeSpec( new SqlIdentifier( castSpec, SqlParserPos.ZERO ), -1, -1, null, null, SqlParserPos.ZERO );
    }


    @Override
    public void unparseOffsetFetch( SqlWriter writer, SqlNode offset, SqlNode fetch ) {
        unparseFetchUsingLimit( writer, offset, fetch );
    }


    @Override
    public void unparseCall( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        switch ( call.getKind() ) {
            default:
                super.unparseCall( writer, call, leftPrec, rightPrec );
        }
    }

}


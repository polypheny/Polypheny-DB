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

package ch.unibas.dmi.dbis.polyphenydb.sql.dialect;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import lombok.extern.slf4j.Slf4j;


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


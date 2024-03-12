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

package org.polypheny.db.sql.language.dialect;


import org.polypheny.db.algebra.constant.NullCollation;
import org.polypheny.db.sql.language.SqlDialect;


/**
 * A <code>SqlDialect</code> implementation for an unknown ANSI compatible database.
 */
public class AnsiSqlDialect extends SqlDialect {

    /**
     * A dialect useful for generating generic SQL. If you need to do something database-specific like quoting identifiers, don't rely on this dialect to do what you want.
     */
    public static final SqlDialect DEFAULT =
            new AnsiSqlDialect( emptyContext()
                    .withNullCollation( NullCollation.HIGH )
                    .withIdentifierQuoteString( "`" ) );


    /**
     * Dialect used for unit tests. Identical to the SqlDialect.DEFAULT, except that it claims to support nested arrays
     */
    public static final SqlDialect NULL_DIALECT =
            new AnsiSqlDialect( emptyContext()
                    .withNullCollation( NullCollation.HIGH )
                    .withIdentifierQuoteString( "`" ) ) {
                @Override
                public boolean supportsNestedArrays() {
                    return true;
                }
            };


    /**
     * Creates an AnsiSqlDialect.
     */
    public AnsiSqlDialect( Context context ) {
        super( context );
    }

}


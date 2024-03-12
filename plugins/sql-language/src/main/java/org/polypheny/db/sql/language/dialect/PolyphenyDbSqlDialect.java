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
 * A <code>SqlDialect</code> implementation that produces SQL that can be parsed by Polypheny-DB.
 */
public class PolyphenyDbSqlDialect extends SqlDialect {

    /**
     * A dialect useful for generating SQL which can be parsed by the Polypheny-DB parser, in particular quoting literals and identifiers.
     * If you want a dialect that knows the full capabilities of the database, create one from a connection.
     */
    public static final SqlDialect DEFAULT =
            new PolyphenyDbSqlDialect( emptyContext()
                    .withIdentifierQuoteString( "\"" )
                    .withNullCollation( NullCollation.HIGH ) );


    /**
     * Creates a PolyphenyDbSqlDialect.
     */
    public PolyphenyDbSqlDialect( Context context ) {
        super( context );
    }


    @Override
    public boolean supportsNestedArrays() {
        return true;
    }


    @Override
    public boolean supportsArrays() {
        return true;
    }

}

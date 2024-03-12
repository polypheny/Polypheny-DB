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
 * A <code>SqlDialect</code> implementation for the Apache Phoenix database.
 */
public class PhoenixSqlDialect extends SqlDialect {

    public static final SqlDialect DEFAULT =
            new PhoenixSqlDialect( EMPTY_CONTEXT
                    .withNullCollation( NullCollation.HIGH )
                    .withIdentifierQuoteString( "\"" ) );


    /**
     * Creates a PhoenixSqlDialect.
     */
    public PhoenixSqlDialect( Context context ) {
        super( context );
    }


    @Override
    public boolean supportsCharSet() {
        return false;
    }

}

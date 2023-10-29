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

package org.polypheny.db.sql.language.ddl;




import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.transaction.Statement;

import static org.polypheny.db.util.Static.RESOURCE;


/**
 * Parse tree for {@code DROP TABLE} statement.
 */
public class SqlDropAlias extends SqlDropObject {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "DROP ALIAS", Kind.DROP_ALIAS );


    /**
     * Creates a SqlDropTable.
     */
    SqlDropAlias(ParserPos pos, boolean ifExists, SqlIdentifier name ) {
        super( OPERATOR, pos, ifExists, name );
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        Catalog catalog = Catalog.getInstance();

        if( name.names.size() != 1 || !catalog.isAlias(name.names.get(0))) {
            throw new RuntimeException(name.toString() + " is not a known alias name");
        }

        catalog.removeAlias(name.names.get(0));

    }

}


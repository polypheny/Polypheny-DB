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

package org.polypheny.db.sql.ddl;


import java.util.List;
import org.polypheny.db.catalog.CatalogManager;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlAlter;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.SqlSpecialOperator;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Transaction;


/**
 * Parse tree for {@code ALTER CATALOG RESET} statement.
 */
public class SqlAlterCatalogReset extends SqlAlter {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "ALTER CATALOG RESET", SqlKind.OTHER_DDL );


    /**
     * Creates a SqlDdl.
     */
    public SqlAlterCatalogReset( SqlParserPos pos ) {
        super( OPERATOR, pos );
    }


    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "CATALOG" );
        writer.keyword( "RESET" );
        super.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Transaction transaction ) {
        CatalogManager.getInstance().getCatalog().clear();
    }
}

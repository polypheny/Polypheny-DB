/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.languages.sql.ddl;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.core.nodes.ExecutableStatement;
import org.polypheny.db.core.enums.Kind;
import org.polypheny.db.core.nodes.Node;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.languages.sql.SqlDdl;
import org.polypheny.db.languages.sql.SqlIdentifier;
import org.polypheny.db.languages.sql.SqlNode;
import org.polypheny.db.languages.sql.SqlOperator;
import org.polypheny.db.languages.sql.SqlSpecialOperator;
import org.polypheny.db.languages.sql.SqlWriter;
import org.polypheny.db.transaction.Statement;


/**
 * Parse tree for {@code TRUNCATE TABLE } statement.
 */
public class SqlTruncate extends SqlDdl implements ExecutableStatement {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "TRUNCATE", Kind.TRUNCATE );

    protected final SqlIdentifier name;


    /**
     * Creates a SqlDropTable.
     */
    public SqlTruncate( ParserPos pos, SqlIdentifier name ) {
        super( OPERATOR, pos );
        this.name = name;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableList.of( name );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableList.of( name );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( getOperator().getName() );
        writer.keyword( "TABLE" );
        name.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        CatalogTable table = getCatalogTable( context, name );

        DdlManager.getInstance().truncate( table, statement );
    }

}


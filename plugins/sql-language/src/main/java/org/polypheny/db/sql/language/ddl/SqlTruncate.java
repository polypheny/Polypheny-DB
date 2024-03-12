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

package org.polypheny.db.sql.language.ddl;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.sql.language.SqlDdl;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.sql.language.SqlWriter;
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
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        LogicalTable table = getTableFailOnEmpty( context, name );

        DdlManager.getInstance().truncate( table, statement );
    }

}


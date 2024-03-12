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


import java.util.List;
import java.util.Objects;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.sql.language.SqlCreate;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code CREATE NAMESPACE} statement (has alias for CREATE SCHEMA).
 */
public class SqlCreateNamespace extends SqlCreate implements ExecutableStatement {

    private final SqlIdentifier name;

    private final DataModel type;

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "CREATE NAMESPACE", Kind.CREATE_NAMESPACE );


    /**
     * Creates a SqlCreateNamespace.
     */
    SqlCreateNamespace( ParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name, DataModel dataModel ) {
        super( OPERATOR, pos, replace, ifNotExists );
        this.name = Objects.requireNonNull( name );
        this.type = dataModel;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( name );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( name );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "CREATE" );
        if ( replace ) {
            writer.keyword( "OR REPLACE" );
        }
        if ( type != null && type != DataModel.RELATIONAL ) {
            writer.keyword( type.name() );
        }
        writer.keyword( "NAMESPACE" );
        if ( ifNotExists ) {
            writer.keyword( "IF NOT EXISTS" );
        }
        name.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        DdlManager.getInstance().createNamespace( name.getSimple(), type, ifNotExists, replace, statement );
    }

}

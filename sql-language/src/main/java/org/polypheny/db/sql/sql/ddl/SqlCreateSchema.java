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

package org.polypheny.db.sql.sql.ddl;


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import java.util.Objects;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.exceptions.SchemaAlreadyExistsException;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.sql.SqlCreate;
import org.polypheny.db.sql.sql.SqlIdentifier;
import org.polypheny.db.sql.sql.SqlNode;
import org.polypheny.db.sql.sql.SqlOperator;
import org.polypheny.db.sql.sql.SqlSpecialOperator;
import org.polypheny.db.sql.sql.SqlWriter;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code CREATE SCHEMA} statement.
 */
public class SqlCreateSchema extends SqlCreate implements ExecutableStatement {

    private final SqlIdentifier name;

    private final SchemaType type;

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "CREATE SCHEMA", Kind.CREATE_SCHEMA );


    /**
     * Creates a SqlCreateSchema.
     */
    SqlCreateSchema( ParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name, SchemaType schemaType ) {
        super( OPERATOR, pos, replace, ifNotExists );
        this.name = Objects.requireNonNull( name );
        this.type = schemaType;
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
        writer.keyword( "SCHEMA" );
        if ( ifNotExists ) {
            writer.keyword( "IF NOT EXISTS" );
        }
        name.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        try {
            DdlManager.getInstance().createSchema( name.getSimple(), context.getDatabaseId(), type, context.getCurrentUserId(), ifNotExists, replace );
        } catch ( SchemaAlreadyExistsException e ) {
            throw CoreUtil.newContextException( name.getPos(), RESOURCE.schemaExists( name.getSimple() ) );
        }

    }

}

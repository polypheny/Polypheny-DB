/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.sql.language.ddl.alterschema;


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import java.util.Objects;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.ddl.SqlAlterSchema;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER SCHEMA name OWNER TO} statement.
 */
public class SqlAlterSchemaOwner extends SqlAlterSchema {

    private final SqlIdentifier schema;
    private final SqlIdentifier owner;


    /**
     * Creates a SqlAlterSchemaOwner.
     */
    public SqlAlterSchemaOwner( ParserPos pos, SqlIdentifier schema, SqlIdentifier owner ) {
        super( pos );
        this.schema = Objects.requireNonNull( schema );
        this.owner = Objects.requireNonNull( owner );
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( schema, owner );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( schema, owner );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "SCHEMA" );
        schema.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "OWNER" );
        writer.keyword( "TO" );
        owner.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        try {
            DdlManager.getInstance().alterSchemaOwner( schema.getSimple(), owner.getSimple(), context.getDatabaseId() );
        } catch ( UnknownSchemaException e ) {
            throw CoreUtil.newContextException( schema.getPos(), RESOURCE.schemaNotFound( schema.getSimple() ) );
        } catch ( UnknownUserException e ) {
            throw CoreUtil.newContextException( owner.getPos(), RESOURCE.userNotFound( owner.getSimple() ) );
        }
    }

}


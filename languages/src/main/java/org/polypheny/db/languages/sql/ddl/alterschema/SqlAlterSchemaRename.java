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

package org.polypheny.db.languages.sql.ddl.alterschema;


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import java.util.Objects;
import org.polypheny.db.catalog.exceptions.SchemaAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.core.util.CoreUtil;
import org.polypheny.db.core.nodes.Node;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.languages.sql.SqlIdentifier;
import org.polypheny.db.languages.sql.SqlNode;
import org.polypheny.db.languages.sql.SqlWriter;
import org.polypheny.db.languages.sql.ddl.SqlAlterSchema;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER SCHEMA name RENAME TO} statement.
 */
public class SqlAlterSchemaRename extends SqlAlterSchema {

    private final SqlIdentifier oldName;
    private final SqlIdentifier newName;


    public SqlAlterSchemaRename( ParserPos pos, SqlIdentifier oldName, SqlIdentifier newName ) {
        super( pos );
        this.oldName = Objects.requireNonNull( oldName );
        this.newName = Objects.requireNonNull( newName );
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( oldName, newName );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( oldName, newName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "SCHEMA" );
        oldName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "RENAME" );
        writer.keyword( "TO" );
        newName.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        try {
            DdlManager.getInstance().renameSchema( newName.getSimple(), oldName.getSimple(), context.getDatabaseId() );
        } catch ( SchemaAlreadyExistsException e ) {
            throw CoreUtil.newContextException( newName.getPos(), RESOURCE.schemaExists( newName.getSimple() ) );
        } catch ( UnknownSchemaException e ) {
            throw CoreUtil.newContextException( oldName.getPos(), RESOURCE.schemaNotFound( oldName.getSimple() ) );
        }
    }

}


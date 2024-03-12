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

package org.polypheny.db.sql.language.ddl.alternamespace;


import java.util.List;
import java.util.Objects;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.ddl.SqlAlterNamespace;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER NAMESPACE name RENAME TO} statement (has alias for ALTER SCHEMA).
 */
public class SqlAlterNamespaceRename extends SqlAlterNamespace {

    private final SqlIdentifier currentName;
    private final SqlIdentifier newName;


    public SqlAlterNamespaceRename( ParserPos pos, SqlIdentifier currentName, SqlIdentifier newName ) {
        super( pos );
        this.currentName = Objects.requireNonNull( currentName );
        this.newName = Objects.requireNonNull( newName );
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( currentName, newName );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( currentName, newName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "NAMESPACE" );
        currentName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "RENAME" );
        writer.keyword( "TO" );
        newName.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        DdlManager.getInstance().renameNamespace( newName.getSimple(), currentName.getSimple() );
    }

}

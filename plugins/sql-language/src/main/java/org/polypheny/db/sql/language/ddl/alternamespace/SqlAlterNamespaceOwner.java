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
import java.util.Map;
import java.util.Objects;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.ddl.SqlAlterNamespace;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.locking.Lockable;
import org.polypheny.db.transaction.locking.Lockable.LockType;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER NAMESPACE name OWNER TO} statement (has alias for ALTER SCHEMA).
 */
public class SqlAlterNamespaceOwner extends SqlAlterNamespace {

    private final SqlIdentifier namespace;
    private final SqlIdentifier owner;


    /**
     * Creates a SqlAlterNamespaceOwner.
     */
    public SqlAlterNamespaceOwner( ParserPos pos, SqlIdentifier namespace, SqlIdentifier owner ) {
        super( pos );
        this.namespace = Objects.requireNonNull( namespace );
        this.owner = Objects.requireNonNull( owner );
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( namespace, owner );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( namespace, owner );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "NAMESPACE" );
        namespace.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "OWNER" );
        writer.keyword( "TO" );
        owner.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        throw new UnsupportedOperationException( "This functionality is not yet supported." );
    }


    @Override
    public Map<Lockable, LockType> deriveLockables( Context context, ParsedQueryContext parsedQueryContext ) {
        return getMapOfNamespaceLockable( namespace, context, LockType.EXCLUSIVE );
    }

}

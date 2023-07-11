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

package org.polypheny.db.sql.language.ddl.altertable;


import java.util.List;
import java.util.Objects;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.ddl.SqlAlterTable;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name DROP FOREIGN KEY} statement.
 */
public class SqlAlterTableDropForeignKey extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier foreignKeyName;


    public SqlAlterTableDropForeignKey( ParserPos pos, SqlIdentifier table, SqlIdentifier foreignKeyName ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.foreignKeyName = Objects.requireNonNull( foreignKeyName );
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( table, foreignKeyName );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( table, foreignKeyName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "DROP" );
        writer.keyword( "FOREIGN" );
        writer.keyword( "KEY" );
        foreignKeyName.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        LogicalTable catalogTable = getEntityFromCatalog( context, table );

        DdlManager.getInstance().dropForeignKey( catalogTable, foreignKeyName.getSimple() );
    }

}


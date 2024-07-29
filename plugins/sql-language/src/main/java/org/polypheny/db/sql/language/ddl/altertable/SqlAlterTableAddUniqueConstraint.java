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

package org.polypheny.db.sql.language.ddl.altertable;


import java.util.List;
import java.util.Objects;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.ddl.SqlAlterTable;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name ADD CONSTRAINT UNIQUE} statement.
 */
public class SqlAlterTableAddUniqueConstraint extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier constraintName;
    private final SqlNodeList columnList;


    public SqlAlterTableAddUniqueConstraint(
            ParserPos pos,
            SqlIdentifier table,
            SqlIdentifier constraintName,
            SqlNodeList columnList ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.constraintName = Objects.requireNonNull( constraintName );
        this.columnList = Objects.requireNonNull( columnList );
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( table, columnList );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( table, columnList );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ADD" );
        writer.keyword( "CONSTRAINT" );
        constraintName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "UNIQUE" );
        columnList.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        LogicalTable logicalTable = getTableFailOnEmpty( context, table );

        if ( logicalTable.entityType != EntityType.ENTITY ) {
            throw new GenericRuntimeException( "Not possible to use ALTER TABLE because " + logicalTable.name + " is not a table." );
        }

        DdlManager.getInstance().createUniqueConstraint(
                logicalTable,
                columnList.getList().stream().map( Node::toString ).toList(),
                constraintName.getSimple(),
                statement );
    }

}


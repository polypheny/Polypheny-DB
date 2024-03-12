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
import org.polypheny.db.catalog.logistic.ForeignKeyOption;
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
 * Parse tree for {@code ALTER TABLE name ADD CONSTRAINT FOREIGN KEY} statement.
 */
public class SqlAlterTableAddForeignKey extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier constraintName;
    private final SqlNodeList columnList;
    private final SqlIdentifier referencesTable;
    private final SqlNodeList referencesList;
    private final ForeignKeyOption onUpdate;
    private final ForeignKeyOption onDelete;


    public SqlAlterTableAddForeignKey( ParserPos pos, SqlIdentifier table, SqlIdentifier constraintName, SqlNodeList columnList, SqlIdentifier referencesTable, SqlNodeList referencesList, String onUpdate, String onDelete ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.constraintName = Objects.requireNonNull( constraintName );
        this.columnList = Objects.requireNonNull( columnList );
        this.referencesTable = Objects.requireNonNull( referencesTable );
        this.referencesList = Objects.requireNonNull( referencesList );
        this.onUpdate = onUpdate != null ? ForeignKeyOption.parse( onUpdate ) : ForeignKeyOption.RESTRICT;
        this.onDelete = onDelete != null ? ForeignKeyOption.parse( onDelete ) : ForeignKeyOption.RESTRICT;

    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( table, constraintName, columnList, referencesList );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( table, constraintName, columnList, referencesList );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ADD" );
        writer.keyword( "CONSTRAINT" );
        constraintName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "FOREIGN" );
        writer.keyword( "KEY" );
        columnList.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "REFERENCES" );
        referencesList.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ON" );
        writer.keyword( "UPDATE" );
        writer.keyword( onUpdate.name() );
        writer.keyword( "ON" );
        writer.keyword( "DELETE" );
        writer.keyword( onDelete.name() );
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        LogicalTable logicalTable = getTableFailOnEmpty( context, table );
        LogicalTable refTable = getTableFailOnEmpty( context, referencesTable );

        if ( logicalTable.entityType != EntityType.ENTITY ) {
            throw new GenericRuntimeException( "Entity " + logicalTable.getNamespaceName() + "." + logicalTable.getName() + " is not an entity, which can used for constraints." );
        }

        if ( refTable.entityType != EntityType.ENTITY ) {
            throw new GenericRuntimeException( "Entity " + refTable.getNamespaceName() + "." + refTable.getName() + " is not an entity, which can used for constraints." );
        }

        DdlManager.getInstance().createForeignKey(
                logicalTable,
                refTable,
                columnList.getList().stream().map( Node::toString ).toList(),
                referencesList.getList().stream().map( Node::toString ).toList(),
                constraintName.getSimple(),
                onUpdate,
                onDelete );

    }

}


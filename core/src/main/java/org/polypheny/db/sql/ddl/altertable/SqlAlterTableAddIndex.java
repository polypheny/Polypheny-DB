/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.sql.ddl.altertable;


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.adapter.IndexManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.IndexType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownIndexException;
import org.polypheny.db.catalog.exceptions.UnknownIndexTypeException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlNodeList;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name ADD INDEX} statement.
 */
public class SqlAlterTableAddIndex extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier indexName;
    private final SqlIdentifier indexType;
    private final SqlNodeList columnList;
    private final boolean unique;


    public SqlAlterTableAddIndex(
            SqlParserPos pos,
            SqlIdentifier table,
            SqlNodeList columnList,
            boolean unique,
            SqlIdentifier indexType,
            SqlIdentifier indexName ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.columnList = Objects.requireNonNull( columnList );
        this.unique = unique;
        this.indexName = indexName;
        this.indexType = indexType;
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( table, columnList, indexType, indexName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ADD" );
        if ( unique ) {
            writer.keyword( "UNIQUE" );
        }
        writer.keyword( "INDEX" );
        indexName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ON" );
        columnList.unparse( writer, leftPrec, rightPrec );
        if ( indexType != null ) {
            writer.keyword( "USING" );
            indexType.unparse( writer, leftPrec, rightPrec );
        }
    }


    @Override
    public void execute( Context context, Statement statement ) {
        CatalogTable catalogTable = getCatalogTable( context, table );
        try {
            List<Long> columnIds = new LinkedList<>();
            for ( SqlNode node : columnList.getList() ) {
                String columnName = node.toString();
                CatalogColumn catalogColumn = Catalog.getInstance().getColumn( catalogTable.id, columnName );
                columnIds.add( catalogColumn.id );
            }
            IndexType type;
            if ( indexType != null ) {
                type = IndexType.parse( indexType.getSimple() );
            } else {
                type = IndexType.getById( RuntimeConfig.DEFAULT_INDEX_TYPE.getInteger() );
            }

            // Check if index already exists
            if ( Catalog.getInstance().checkIfExistsIndex( catalogTable.id, indexName.getSimple() ) ) {
                throw SqlUtil.newContextException( indexName.getParserPosition(), RESOURCE.indexExists( indexName.getSimple() ) );
            }

            final long indexId = Catalog.getInstance().addIndex( catalogTable.id, columnIds, unique, type, indexName.getSimple() );
            IndexManager.getInstance().addIndex( Catalog.getInstance().getIndex( indexId ), transaction );
        } catch ( GenericCatalogException | UnknownColumnException | UnknownIndexTypeException | UnknownIndexException | UnknownSchemaException | UnknownTableException | UnknownKeyException | UnknownUserException | UnknownDatabaseException | TransactionException e ) {
            throw new RuntimeException( e );
        }
    }

}


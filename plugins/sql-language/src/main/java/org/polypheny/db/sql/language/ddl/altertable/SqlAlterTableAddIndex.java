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


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.index.IndexManager;
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
import org.polypheny.db.transaction.locking.Lockable;
import org.polypheny.db.transaction.locking.Lockable.LockType;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name ADD INDEX} statement.
 */
public class SqlAlterTableAddIndex extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier indexName;
    private final SqlIdentifier indexMethod;
    private final SqlNodeList columnList;
    private final boolean unique;
    private final SqlIdentifier storeName;


    public SqlAlterTableAddIndex(
            ParserPos pos,
            SqlIdentifier table,
            SqlNodeList columnList,
            boolean unique,
            SqlIdentifier indexMethod,
            SqlIdentifier indexName,
            SqlIdentifier storeName ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.columnList = Objects.requireNonNull( columnList );
        this.unique = unique;
        this.indexName = indexName;
        this.indexMethod = indexMethod;
        this.storeName = storeName;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( table, columnList, storeName, indexMethod, indexName );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( table, columnList, storeName, indexMethod, indexName );
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
        if ( indexMethod != null ) {
            writer.keyword( "USING" );
            indexMethod.unparse( writer, leftPrec, rightPrec );
        }
        if ( storeName != null ) {
            writer.keyword( "ON" );
            writer.keyword( "STORE" );
            storeName.unparse( writer, leftPrec, rightPrec );
        }
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        LogicalTable logicalTable = getTableFailOnEmpty( context, table );

        if ( logicalTable.entityType != EntityType.ENTITY && logicalTable.entityType != EntityType.MATERIALIZED_VIEW ) {
            throw new GenericRuntimeException( "Not possible to use ALTER TABLE ADD INDEX because " + logicalTable.name + " is not a table or materialized view." );
        }

        String indexMethodName = indexMethod != null ? indexMethod.getSimple() : null;

        if ( storeName != null && storeName.getSimple().equalsIgnoreCase( IndexManager.POLYPHENY ) ) {
            DdlManager.getInstance().createPolyphenyIndex(
                    logicalTable,
                    indexMethodName,
                    columnList.getList().stream().map( Node::toString ).toList(),
                    indexName.getSimple(),
                    unique,
                    statement );
        } else {
            DataStore<?> storeInstance = null;
            if ( storeName != null ) {
                storeInstance = getDataStoreInstance( storeName );
                if ( storeInstance == null ) {
                    throw CoreUtil.newContextException(
                            storeName.getPos(),
                            RESOURCE.unknownAdapter( storeName.getSimple() ) );
                }
            }
            DdlManager.getInstance().createIndex(
                    logicalTable,
                    indexMethodName,
                    columnList.getList().stream().map( Node::toString ).toList(),
                    indexName.getSimple(),
                    unique,
                    storeInstance,
                    statement );
        }
    }


    @Override
    public Map<Lockable, LockType> deriveLockables( Context context, ParsedQueryContext parsedQueryContext ) {
        return getMapOfTableLockable( table, context, LockType.EXCLUSIVE );
    }

}

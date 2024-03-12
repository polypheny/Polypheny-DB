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

package org.polypheny.db.sql.language.ddl.altermaterializedview;

import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.index.IndexManager;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.ddl.SqlAlterMaterializedView;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.ImmutableNullableList;

@EqualsAndHashCode(callSuper = true)
@Value
public class SqlAlterMaterializedViewAddIndex extends SqlAlterMaterializedView {


    SqlIdentifier table;
    SqlIdentifier indexName;
    SqlIdentifier indexMethod;
    SqlNodeList columnList;
    boolean unique;
    SqlIdentifier storeName;


    public SqlAlterMaterializedViewAddIndex(
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
        writer.keyword( "MATERIALIZED VIEW" );
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
        LogicalTable table = getTableFailOnEmpty( context, this.table );
        String indexMethodName = indexMethod != null ? indexMethod.getSimple() : null;

        try {
            if ( storeName != null && storeName.getSimple().equalsIgnoreCase( IndexManager.POLYPHENY ) ) {
                DdlManager.getInstance().createPolyphenyIndex(
                        table,
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
                        table,
                        indexMethodName,
                        columnList.getList().stream().map( Node::toString ).toList(),
                        indexName.getSimple(),
                        unique,
                        storeInstance,
                        statement );
            }
        } catch ( TransactionException e ) {
            throw new GenericRuntimeException( e );
        }
    }

}

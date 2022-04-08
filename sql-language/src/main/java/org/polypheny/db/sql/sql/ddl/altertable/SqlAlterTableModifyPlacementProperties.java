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

package org.polypheny.db.sql.sql.ddl.altertable;


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownPlacementStateException;
import org.polypheny.db.catalog.exceptions.UnknownReplicationStrategyException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.LastPlacementException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.replication.properties.exception.InvalidPlacementPropertySpecification;
import org.polypheny.db.replication.properties.exception.UnknownPlacementPropertyException;
import org.polypheny.db.sql.sql.SqlIdentifier;
import org.polypheny.db.sql.sql.SqlNode;
import org.polypheny.db.sql.sql.SqlWriter;
import org.polypheny.db.sql.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.sql.util.replication.properties.SqlPlacementPropertyExtractor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name MODIFY PLACEMENT SET ( propertyName propertyValue [, propertyName propertyValue]* ) } statement.
 */
@Slf4j
public class SqlAlterTableModifyPlacementProperties extends SqlAlterTable {


    private final SqlIdentifier table;
    private final SqlIdentifier storeName;
    private final Map<SqlIdentifier, SqlIdentifier> placementPropertyMap;


    public SqlAlterTableModifyPlacementProperties(
            ParserPos pos,
            SqlIdentifier table,
            SqlIdentifier storeName,
            Map<SqlIdentifier, SqlIdentifier> placementPropertyMap ) {

        super( pos );
        this.table = Objects.requireNonNull( table );
        this.storeName = Objects.requireNonNull( storeName );
        this.placementPropertyMap = Objects.requireNonNull( placementPropertyMap );
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( table, storeName );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( table, storeName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "MODIFY" );
        writer.keyword( "PLACEMENT" );
        writer.keyword( "ON" );
        writer.keyword( "STORE" );
        writer.keyword( "SET" );
        storeName.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {

        CatalogTable catalogTable = getCatalogTable( context, table );

        if ( catalogTable.tableType != TableType.TABLE ) {
            throw new RuntimeException( "Not possible to use ALTER TABLE because " + catalogTable.name + " is not a table." );
        }

        if ( placementPropertyMap.isEmpty() ) {
            throw new RuntimeException( "Empty property mapping is not allowed." );
        }

        DataStore storeInstance = AdapterManager.getInstance().getStore( storeName.getSimple() );
        if ( storeInstance == null ) {
            throw CoreUtil.newContextException(
                    storeName.getPos(),
                    RESOURCE.unknownStoreName( storeName.getSimple() ) );
        }
        int storeId = storeInstance.getAdapterId();
        // Check whether this placement already exists
        if ( !catalogTable.dataPlacements.contains( storeId ) ) {
            throw CoreUtil.newContextException(
                    storeName.getPos(),
                    RESOURCE.placementDoesNotExist( storeName.getSimple(), catalogTable.name ) );
        }

        // TODO @HENNLO Check constraints ( sufficient primary etc. )

        // Update
        try {
            DdlManager.getInstance().modifyDataPlacementProperties(
                    SqlPlacementPropertyExtractor.fromNodeLists( catalogTable, placementPropertyMap ),
                    storeInstance,
                    statement
            );
        } catch ( LastPlacementException | UnknownPlacementStateException | UnknownPlacementPropertyException | InvalidPlacementPropertySpecification | UnknownReplicationStrategyException e ) {
            throw new RuntimeException( "Failed to execute requested partition modification. This change would remove one partition entirely from table '" + catalogTable.name + "'", e );
        }
    }

}

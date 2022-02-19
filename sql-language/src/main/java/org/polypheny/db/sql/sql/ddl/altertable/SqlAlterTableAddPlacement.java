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

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.PlacementAlreadyExistsException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.sql.SqlIdentifier;
import org.polypheny.db.sql.sql.SqlNode;
import org.polypheny.db.sql.sql.SqlNodeList;
import org.polypheny.db.sql.sql.SqlWriter;
import org.polypheny.db.sql.sql.ddl.SqlAlterTable;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name ADD PLACEMENT [(columnList)] ON STORE storeName} statement.
 */
@Slf4j
public class SqlAlterTableAddPlacement extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlNodeList columnList;
    private final SqlIdentifier storeName;
    private final List<Integer> partitionGroupsList;
    private final List<SqlIdentifier> partitionGroupNamesList;


    public SqlAlterTableAddPlacement(
            ParserPos pos,
            SqlIdentifier table,
            SqlNodeList columnList,
            SqlIdentifier storeName,
            List<Integer> partitionGroupsList,
            List<SqlIdentifier> partitionGroupNamesList ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.columnList = Objects.requireNonNull( columnList );
        this.storeName = Objects.requireNonNull( storeName );
        this.partitionGroupsList = partitionGroupsList;
        this.partitionGroupNamesList = partitionGroupNamesList;

    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( table, columnList, storeName );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( table, columnList, storeName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ADD" );
        writer.keyword( "PLACEMENT" );
        columnList.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ON" );
        writer.keyword( "STORE" );
        storeName.unparse( writer, leftPrec, rightPrec );

        if ( partitionGroupsList != null || partitionGroupNamesList != null ) {
            writer.keyword( " WITH " );
            writer.keyword( " PARTITIONS" );
            SqlWriter.Frame frame = writer.startList( "(", ")" );

            if ( partitionGroupNamesList != null ) {
                for ( int i = 0; i < partitionGroupNamesList.size(); i++ ) {
                    partitionGroupNamesList.get( i ).unparse( writer, leftPrec, rightPrec );
                    if ( i + 1 < partitionGroupNamesList.size() ) {
                        writer.sep( "," );
                    }
                }
            }
        }
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        CatalogTable catalogTable = getCatalogTable( context, table );
        DataStore storeInstance = getDataStoreInstance( storeName );

        if ( catalogTable.tableType != TableType.TABLE ) {
            throw new RuntimeException( "Not possible to use ALTER TABLE because " + catalogTable.name + " is not a table." );
        }

        // You can't partition placements if the table is not partitioned
        if ( !catalogTable.partitionProperty.isPartitioned && (!partitionGroupsList.isEmpty() || !partitionGroupNamesList.isEmpty()) ) {
            throw new RuntimeException( "Partition Placement is not allowed for unpartitioned table '" + catalogTable.name + "'" );
        }

        List<Long> columnIds = new LinkedList<>();
        for ( SqlNode node : columnList.getSqlList() ) {
            CatalogColumn catalogColumn = getCatalogColumn( catalogTable.id, (SqlIdentifier) node );
            columnIds.add( catalogColumn.id );
        }

        try {
            DdlManager.getInstance().addDataPlacement(
                    catalogTable,
                    columnIds,
                    partitionGroupsList,
                    partitionGroupNamesList.stream().map( SqlIdentifier::toString ).collect( Collectors.toList() ),
                    storeInstance,
                    statement );
        } catch ( PlacementAlreadyExistsException e ) {
            throw CoreUtil.newContextException(
                    storeName.getPos(),
                    RESOURCE.placementAlreadyExists( catalogTable.name, storeName.getSimple() ) );
        }

    }

}

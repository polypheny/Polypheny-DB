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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogPartitionGroup;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.LastPlacementException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.sql.SqlIdentifier;
import org.polypheny.db.sql.sql.SqlNode;
import org.polypheny.db.sql.sql.SqlWriter;
import org.polypheny.db.sql.sql.ddl.SqlAlterTable;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name MODIFY PARTITIONS (partitionId [, partitionId]* ) } statement.
 */
@Slf4j
public class SqlAlterTableModifyPartitions extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier storeName;
    private final List<Integer> partitionGroupList;
    private final List<SqlIdentifier> partitionGroupNamesList;


    public SqlAlterTableModifyPartitions(
            ParserPos pos,
            SqlIdentifier table,
            SqlIdentifier storeName,
            List<Integer> partitionGroupList,
            List<SqlIdentifier> partitionGroupNamesList ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.storeName = Objects.requireNonNull( storeName );
        this.partitionGroupList = partitionGroupList;
        this.partitionGroupNamesList = partitionGroupNamesList; //May be null and can only be used in association with PARTITION BY and PARTITIONS
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
        writer.keyword( "PARTITIONS" );
        writer.keyword( "ON" );
        writer.keyword( "STORE" );
        storeName.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        Catalog catalog = Catalog.getInstance();
        CatalogTable catalogTable = getCatalogTable( context, table );

        if ( catalogTable.tableType != TableType.TABLE ) {
            throw new RuntimeException( "Not possible to use ALTER TABLE because " + catalogTable.name + " is not a table." );
        }

        if ( !catalogTable.partitionProperty.isPartitioned ) {
            throw new RuntimeException( "Table '" + catalogTable.name + "' is not partitioned" );
        }

        long tableId = catalogTable.id;

        if ( partitionGroupList.isEmpty() && partitionGroupNamesList.isEmpty() ) {
            throw new RuntimeException( "Empty Partition Placement is not allowed for partitioned table '" + catalogTable.name + "'" );
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

        List<Long> tempPartitionList = new ArrayList<>();

        // If index partitions are specified
        if ( !partitionGroupList.isEmpty() && partitionGroupNamesList.isEmpty() ) {
            //First convert specified index to correct partitionId
            for ( int partitionId : partitionGroupList ) {
                // Check if specified partition index is even part of table and if so get corresponding uniquePartId
                try {
                    tempPartitionList.add( catalogTable.partitionProperty.partitionGroupIds.get( partitionId ) );
                } catch ( IndexOutOfBoundsException e ) {
                    throw new RuntimeException( "Specified Partition-Index: '" + partitionId + "' is not part of table '"
                            + catalogTable.name + "', has only " + catalogTable.partitionProperty.numPartitionGroups + " partitions" );
                }
            }
        }
        // If name partitions are specified
        else if ( !partitionGroupNamesList.isEmpty() && partitionGroupList.isEmpty() ) {
            List<CatalogPartitionGroup> catalogPartitionGroups = catalog.getPartitionGroups( tableId );
            for ( String partitionName : partitionGroupNamesList.stream().map( Object::toString )
                    .collect( Collectors.toList() ) ) {
                boolean isPartOfTable = false;
                for ( CatalogPartitionGroup catalogPartitionGroup : catalogPartitionGroups ) {
                    if ( partitionName.equals( catalogPartitionGroup.partitionGroupName.toLowerCase() ) ) {
                        tempPartitionList.add( catalogPartitionGroup.id );
                        isPartOfTable = true;
                        break;
                    }
                }
                if ( !isPartOfTable ) {
                    throw new RuntimeException( "Specified Partition-Name: '" + partitionName + "' is not part of table '"
                            + catalogTable.name + "', has only " + catalog.getPartitionGroupNames( tableId ) + " partitions" );
                }
            }
        }

        // Check if in-memory dataPartitionPlacement Map should even be changed and therefore start costly partitioning
        // Avoid unnecessary partitioning when the placement is already partitioned in the same way it has been specified
        if ( tempPartitionList.equals( catalog.getPartitionGroupsOnDataPlacement( storeId, tableId ) ) ) {
            log.info( "The data placement for table: '{}' on store: '{}' already contains all specified partitions of statement: {}",
                    catalogTable.name, storeName, partitionGroupList );
            return;
        }
        // Update
        try {
            DdlManager.getInstance().modifyPartitionPlacement(
                    catalogTable,
                    tempPartitionList,
                    storeInstance,
                    statement
            );
        } catch ( LastPlacementException e ) {
            throw new RuntimeException( "Failed to execute requested partition modification. This change would remove one partition entirely from table '" + catalogTable.name + "'", e );
        }
    }

}

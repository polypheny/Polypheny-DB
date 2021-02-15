/*
 * Copyright 2019-2021 The Polypheny Project
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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.processing.DataMigrator;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlNodeList;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name ADD PLACEMENT [(columnList)] ON STORE storeName} statement.
 */
@Slf4j
public class SqlAlterTableAddPlacement extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlNodeList columnList;
    private final SqlIdentifier storeName;
    private final List<Integer> partitionList;
    private final List<SqlIdentifier> partitionNamesList;


    public SqlAlterTableAddPlacement(
            SqlParserPos pos,
            SqlIdentifier table,
            SqlNodeList columnList,
            SqlIdentifier storeName,
            List<Integer> partitionList,
            List<SqlIdentifier> partitionNamesList ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.columnList = Objects.requireNonNull( columnList );
        this.storeName = Objects.requireNonNull( storeName );
        this.partitionList = partitionList;
        this.partitionNamesList = partitionNamesList;

    }


    @Override
    public List<SqlNode> getOperandList() {
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
    }


    @Override
    public void execute( Context context, Statement statement ) {
        CatalogTable catalogTable = getCatalogTable( context, table );
        Catalog catalog = Catalog.getInstance();
        // You can't partition placements if the table is not partitioned
        if ( !catalogTable.isPartitioned && (!partitionList.isEmpty() || !partitionNamesList.isEmpty()) ) {
            throw new RuntimeException( "Partition Placement is not allowed for unpartitioned table '" + catalogTable.name + "'" );
        }

        List<Long> columnIds = new LinkedList<>();
        for ( SqlNode node : columnList.getList() ) {
            CatalogColumn catalogColumn = getCatalogColumn( catalogTable.id, (SqlIdentifier) node );
            columnIds.add( catalogColumn.id );
        }
        List<CatalogColumn> addedColumns = new LinkedList<>();
        DataStore storeInstance = getDataStoreInstance( storeName );
        // Check whether this placement already exists
        for ( int storeId : catalogTable.placementsByAdapter.keySet() ) {
            if ( storeId == storeInstance.getAdapterId() ) {
                throw SqlUtil.newContextException(
                        storeName.getParserPosition(),
                        RESOURCE.placementAlreadyExists( catalogTable.name, storeName.getSimple() ) );
            }
        }
        // Check whether the list is empty (this is a short hand for a full placement)
        if ( columnIds.size() == 0 ) {
            columnIds = ImmutableList.copyOf( catalogTable.columnIds );
        }

        List<Long> tempPartitionList = new ArrayList<>();
        // Select partitions to create on this placement
        if ( catalogTable.isPartitioned ) {
            boolean isDataPlacementPartitioned = false;
            long tableId = catalogTable.id;
            // Needed to ensure that column placements on the same store contain all the same partitions
            // Check if this column placement is the first on the data placement
            // If this returns null this means that this is the first placement and partition list can therefore be specified
            List<Long> currentPartList = new ArrayList<>();
            currentPartList = catalog.getPartitionsOnDataPlacement( storeInstance.getAdapterId(), catalogTable.id );

            if ( !currentPartList.isEmpty() ) {
                isDataPlacementPartitioned = true;
            } else {
                isDataPlacementPartitioned = false;
            }

            if ( !partitionList.isEmpty() && partitionNamesList.isEmpty() ) {

                // Abort if a manual partitionList has been specified even though the data placement has already been partitioned
                if ( isDataPlacementPartitioned ) {
                    throw new RuntimeException( "WARNING: The Data Placement for table: '" + catalogTable.name + "' on store: '"
                            + storeName + "' already contains manually specified partitions: " + currentPartList + ". Use 'ALTER TABLE ... MODIFY PARTITIONS...' instead" );
                }

                log.debug( "Table is partitioned and concrete partitionList has been specified " );
                // First convert specified index to correct partitionId
                for ( int partitionId : partitionList ) {
                    // Check if specified partition index is even part of table and if so get corresponding uniquePartId
                    try {
                        tempPartitionList.add( catalogTable.partitionIds.get( partitionId ) );
                    } catch ( IndexOutOfBoundsException e ) {
                        throw new RuntimeException( "Specified Partition-Index: '" + partitionId + "' is not part of table '"
                                + catalogTable.name + "', has only " + catalogTable.numPartitions + " partitions" );
                    }
                }
            } else if ( !partitionNamesList.isEmpty() && partitionList.isEmpty() ) {

                if ( isDataPlacementPartitioned ) {
                    throw new RuntimeException( "WARNING: The Data Placement for table: '" + catalogTable.name + "' on store: '"
                            + storeName + "' already contains manually specified partitions: " + currentPartList + ". Use 'ALTER TABLE ... MODIFY PARTITIONS...' instead" );
                }

                List<CatalogPartition> catalogPartitions = catalog.getPartitions( tableId );
                for ( String partitionName : partitionNamesList.stream().map( Object::toString )
                        .collect( Collectors.toList() ) ) {
                    boolean isPartOfTable = false;
                    for ( CatalogPartition catalogPartition : catalogPartitions ) {
                        if ( partitionName.equals( catalogPartition.partitionName.toLowerCase() ) ) {
                            tempPartitionList.add( catalogPartition.id );
                            isPartOfTable = true;
                            break;
                        }
                    }
                    if ( !isPartOfTable ) {
                        throw new RuntimeException( "Specified Partition-Name: '" + partitionName + "' is not part of table '"
                                + catalogTable.name + "', has only " + catalog.getPartitionNames( tableId ) + " partitions" );

                    }
                }
            }
            // Simply Place all partitions on placement since nothing has been specified
            else if ( partitionList.isEmpty() && partitionNamesList.isEmpty() ) {
                log.debug( "Table is partitioned and concrete partitionList has NOT been specified " );

                if ( isDataPlacementPartitioned ) {
                    // If DataPlacement already contains partitions then create new placement with same set of partitions.
                    tempPartitionList = currentPartList;
                } else {
                    tempPartitionList = catalogTable.partitionIds;
                }
            }
        }

        // Create column placements
        for ( long cid : columnIds ) {
            Catalog.getInstance().addColumnPlacement(
                    storeInstance.getAdapterId(),
                    cid,
                    PlacementType.MANUAL,
                    null,
                    null,
                    null,
                    tempPartitionList );

            addedColumns.add( Catalog.getInstance().getColumn( cid ) );
        }
        // Check if placement includes primary key columns
        CatalogPrimaryKey primaryKey = Catalog.getInstance().getPrimaryKey( catalogTable.primaryKey );
        for ( long cid : primaryKey.columnIds ) {
            if ( !columnIds.contains( cid ) ) {
                Catalog.getInstance().addColumnPlacement(
                        storeInstance.getAdapterId(),
                        cid,
                        PlacementType.AUTOMATIC,
                        null,
                        null,
                        null,
                        tempPartitionList );
                addedColumns.add( Catalog.getInstance().getColumn( cid ) );
            }
        }

        // Create table on store
        storeInstance.createTable( context, catalogTable );

        // Copy data to the newly added placements
        DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
        dataMigrator.copyData( statement.getTransaction(), Catalog.getInstance().getAdapter( storeInstance.getAdapterId() ), addedColumns );
    }

}


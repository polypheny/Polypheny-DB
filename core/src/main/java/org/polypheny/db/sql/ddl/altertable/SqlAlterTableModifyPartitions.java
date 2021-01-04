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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownStoreException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name MODIFY PARTITIONS (partitionId [, partitonId]* ) } statement.
 */
@Slf4j
public class SqlAlterTableModifyPartitions extends SqlAlterTable {


    private final SqlIdentifier table;
    private final SqlIdentifier storeName;
    private final List<Integer> partitionList;
    private final List<SqlIdentifier> partitionNamesList;


    public SqlAlterTableModifyPartitions(
            SqlParserPos pos,
            SqlIdentifier table,
            SqlIdentifier storeName,
            List<Integer> partitionList,
            List<SqlIdentifier> partitionNamesList ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.storeName = Objects.requireNonNull( storeName );
        this.partitionList = partitionList;
        this.partitionNamesList = partitionNamesList; //May be null and can only be used in association with PARTITION BY and PARTITIONS
    }


    @Override
    public List<SqlNode> getOperandList() {
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
    public void execute( Context context, Statement statement ) {
        Catalog catalog = Catalog.getInstance();
        CatalogTable catalogTable = getCatalogTable( context, table );
        if ( !catalogTable.isPartitioned ) {
            throw new RuntimeException( "Table '" + catalogTable.name + "' is not partitioned" );
        }

        try {
            // Check if table is already partitioned

            long tableId = catalogTable.id;

            if ( partitionList.isEmpty() && partitionNamesList.isEmpty() ) {
                throw new RuntimeException( "Empty Partition Placement is not allowed for partitioned table '" + catalogTable.name + "'" );
            }

            Store storeInstance = StoreManager.getInstance().getStore( storeName.getSimple() );
            if ( storeInstance == null ) {
                throw SqlUtil.newContextException(
                        storeName.getParserPosition(),
                        RESOURCE.unknownStoreName( storeName.getSimple() ) );
            }
            int storeId = storeInstance.getStoreId();
            // Check whether this placement already exists
            if ( !catalogTable.placementsByStore.containsKey( storeId ) ) {
                throw SqlUtil.newContextException(
                        storeName.getParserPosition(),
                        RESOURCE.placementDoesNotExist( storeName.getSimple(), catalogTable.name ) );
            }
            // Check whether the store supports schema changes
            if ( storeInstance.isSchemaReadOnly() ) {
                throw SqlUtil.newContextException(
                        storeName.getParserPosition(),
                        RESOURCE.storeIsSchemaReadOnly( storeName.getSimple() ) );
            }

            List<Long> tempPartitionList = new ArrayList<>();

            // If index partitions are specified
            if ( !partitionList.isEmpty() && partitionNamesList.isEmpty() ) {
                //First convert specified index to correct partitionId
                for ( int partitionId : partitionList ) {
                    // Check if specified partition index is even part of table and if so get corresponding uniquePartId
                    try {
                        tempPartitionList.add( catalogTable.partitionIds.get( partitionId ) );
                    } catch ( IndexOutOfBoundsException e ) {
                        throw new RuntimeException( "Specified Partition-Index: '" + partitionId + "' is not part of table '"
                                + catalogTable.name + "', has only " + catalogTable.numPartitions + " partitions" );
                    }
                }
            }
            // If name partitions are specified
            else if ( !partitionNamesList.isEmpty() && partitionList.isEmpty() ) {
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

            // Check if inmemory dataPartitionPlacement Map should even be changed and therefore start costly partitioning
            // Avoid unnecessary partitioning when the placement is already partitioned in the same way it has been specified
            if ( tempPartitionList.equals( catalog.getPartitionsOnDataPlacement( storeId, tableId ) ) ) {
                log.info( "The Data Placement for table: '{}' on store: '{}' already contains all specified partitions of statement: {}", catalogTable.name, storeName, partitionList );
                return;
            }
            //Update
            catalog.updatePartitionsOnDataPlacement( storeId, tableId, tempPartitionList );

        } catch ( UnknownTableException | UnknownStoreException e ) {
            throw new RuntimeException( e );
        }
    }
}

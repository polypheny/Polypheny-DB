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


import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionTypeException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name PARTITION BY partitionType (columnName) [PARTITIONS amount]} statement.
 */
@Slf4j
public class SqlAlterTableAddPartitions extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier partitionColumn;
    private final SqlIdentifier partitionType;
    private final int numPartitions;
    private final List<SqlIdentifier> partitionNamesList;
    private final List<List<SqlNode>> partitionQualifierList;


    public SqlAlterTableAddPartitions(
            SqlParserPos pos,
            SqlIdentifier table,
            SqlIdentifier partitionColumn,
            SqlIdentifier partitionType,
            int numPartitions,
            List<SqlIdentifier> partitionNamesList,
            List<List<SqlNode>> partitionQualifierList ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.partitionType = Objects.requireNonNull( partitionType );
        this.partitionColumn = Objects.requireNonNull( partitionColumn );
        this.numPartitions = numPartitions; //May be empty
        this.partitionNamesList = partitionNamesList; //May be null and can only be used in association with PARTITION BY and PARTITIONS
        this.partitionQualifierList = partitionQualifierList;
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( table, partitionType, partitionColumn );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        // TODO @HENNLO: The partition part is still incomplete
        /** There are several possible ways to unparse the partition section.
         The To Do is deferred until we have decided if parsing of partition functions will be
         self contained or not.*/
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "PARTITION" );
        writer.keyword( "BY" );
        partitionType.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement ) {
        CatalogTable catalogTable = getCatalogTable( context, table );
        try {
            // Check if table is already partitioned
            if ( catalogTable.partitionType == Catalog.PartitionType.NONE ) {
                partitionTable(
                        catalogTable.id,
                        partitionType,
                        getCatalogColumn( catalogTable.id, partitionColumn ),
                        partitionNamesList,
                        numPartitions,
                        partitionQualifierList );
            } else {
                throw new RuntimeException( "Table '" + catalogTable.name + "' is already partitioned" );
            }
        } catch ( UnknownPartitionTypeException | GenericCatalogException e ) {
            throw new RuntimeException( e );
        }
    }


    public static void partitionTable( long tableId, SqlIdentifier partitionType, CatalogColumn partitionColumn, List<SqlIdentifier> partitionNamesList, int numberOfPartitions, List<List<SqlNode>> partitionQualifierList ) throws UnknownPartitionTypeException, GenericCatalogException {
        Catalog catalog = Catalog.getInstance();
        CatalogTable catalogTable = catalog.getTable( tableId );

        Catalog.PartitionType actualPartitionType = Catalog.PartitionType.getByName( partitionType.getSimple() );

        // Check if specified partitionColumn is even part of the table
        if ( log.isDebugEnabled() ) {
            log.debug( "Creating partition for table: {} with id {} on schema: {} on column: {}", catalogTable.name, catalogTable.id, catalogTable.getSchemaName(), partitionColumn.id );
        }

        // Get partition manager
        PartitionManagerFactory partitionManagerFactory = new PartitionManagerFactory();
        PartitionManager partitionManager = partitionManagerFactory.getInstance( actualPartitionType );

        // Check whether partition function supports type of partition column
        if ( !partitionManager.supportsColumnOfType( partitionColumn.type ) ) {
            throw new RuntimeException( "The partition function " + actualPartitionType + " does not support columns of type " + partitionColumn.type );
        }

        // Convert partition names and check whether they are unique
        List<String> partitionNames = new ArrayList<>( partitionNamesList.size() );
        for ( SqlIdentifier identifier : partitionNamesList ) {
            String name = identifier.getSimple().trim().toLowerCase();
            if ( partitionNames.contains( name ) ) {
                throw new RuntimeException( "Partition names must be unique for a table! Found duplicate name: " + name );
            }
            partitionNames.add( name );
        }

        // Calculate how many partitions exist if partitioning is applied.
        long partId;
        if ( partitionNames.size() >= 2 && numberOfPartitions == 0 ) {
            numberOfPartitions = partitionNames.size();
        }

        if ( partitionManager.requiresUnboundPartition() ) {
            // Because of the implicit unbound partition
            numberOfPartitions = partitionNames.size();
            numberOfPartitions += 1;
        }

        // Validate partition setup
        List<List<String>> partitionQualifierStringList = new ArrayList<>();
        for ( List<SqlNode> partitionValueList : partitionQualifierList ) {
            partitionQualifierStringList.add( partitionValueList.stream().map( Object::toString ).collect( Collectors.toList() ) );
        }
        if ( !partitionManager.validatePartitionSetup( partitionQualifierStringList, numberOfPartitions, partitionNames, partitionColumn ) ) {
            throw new RuntimeException( "Partitioning failed for table: " + catalogTable.name );
        }

        // Loop over value to create those partitions with partitionKey to uniquelyIdentify partition
        List<Long> partitionIds = new ArrayList<>();
        for ( int i = 0; i < numberOfPartitions; i++ ) {
            String partitionName;

            // Make last partition unbound partition
            if ( partitionManager.requiresUnboundPartition() && i == numberOfPartitions - 1 ) {
                partId = catalog.addPartition(
                        tableId,
                        "Unbound",
                        catalogTable.schemaId,
                        catalogTable.ownerId,
                        actualPartitionType,
                        new ArrayList<>(),
                        true );
            } else {
                // If no names have been explicitly defined
                if ( partitionNames.isEmpty() ) {
                    partitionName = "part_" + i;
                } else {
                    partitionName = partitionNames.get( i );
                }

                // Mainly needed for HASH
                if ( partitionQualifierStringList.isEmpty() ) {
                    partId = catalog.addPartition(
                            tableId,
                            partitionName,
                            catalogTable.schemaId,
                            catalogTable.ownerId,
                            actualPartitionType,
                            new ArrayList<>(),
                            false );
                } else {
                    //partId = catalog.addPartition( tableId, partitionName, old.schemaId, old.ownerId, partitionType, new ArrayList<>( Collections.singletonList( partitionQualifiers.get( i ) ) ), false );
                    partId = catalog.addPartition(
                            tableId,
                            partitionName,
                            catalogTable.schemaId,
                            catalogTable.ownerId,
                            actualPartitionType,
                            partitionQualifierStringList.get( i ),
                            false );
                }
            }
            partitionIds.add( partId );
        }

        // Update catalog table
        catalog.partitionTable( tableId, actualPartitionType, partitionColumn.id, numberOfPartitions, partitionIds );

        // Get primary key of table and use PK to find all DataPlacements of table
        long pkid = catalogTable.primaryKey;
        List<Long> pkColumnIds = catalog.getPrimaryKey( pkid ).columnIds;
        // Basically get first part of PK even if its compound of PK it is sufficient
        CatalogColumn pkColumn = catalog.getColumn( pkColumnIds.get( 0 ) );
        // This gets us only one ccp per store (first part of PK)
        for ( CatalogColumnPlacement ccp : catalog.getColumnPlacements( pkColumn.id ) ) {
            catalog.updatePartitionsOnDataPlacement( ccp.adapterId, ccp.tableId, partitionIds );
        }
    }

}

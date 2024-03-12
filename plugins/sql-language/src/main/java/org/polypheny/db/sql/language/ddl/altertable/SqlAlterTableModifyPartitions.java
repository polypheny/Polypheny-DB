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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.entity.allocation.AllocationPartition;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
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
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.ddl.SqlAlterTable;
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
    private final List<Integer> partitionGroups;
    private final List<SqlIdentifier> partitionGroupNames;


    public SqlAlterTableModifyPartitions(
            ParserPos pos,
            SqlIdentifier table,
            SqlIdentifier storeName,
            List<Integer> partitionGroups,
            List<SqlIdentifier> partitionGroupNames ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.storeName = Objects.requireNonNull( storeName );
        this.partitionGroups = partitionGroups;
        this.partitionGroupNames = partitionGroupNames; //May be null and can only be used in association with PARTITION BY and PARTITIONS
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
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        LogicalTable table = getTableFailOnEmpty( context, this.table );

        if ( table.entityType != EntityType.ENTITY ) {
            throw new GenericRuntimeException( "Not possible to use ALTER TABLE because " + table.name + " is not a table." );
        }

        List<AllocationPartition> partitions = statement.getTransaction().getSnapshot().alloc().getPartitionsFromLogical( table.id );
        if ( partitions.size() < 2 ) {
            throw new GenericRuntimeException( "Table '%s' is not partitioned", table.name );
        }

        if ( partitionGroups.isEmpty() && partitionGroupNames.isEmpty() ) {
            throw new GenericRuntimeException( "Empty Partition Placement is not allowed for partitioned table '" + table.name + "'" );
        }

        Optional<DataStore<?>> optStoreInstance = AdapterManager.getInstance().getStore( storeName.getSimple() );
        if ( optStoreInstance.isEmpty() ) {
            throw CoreUtil.newContextException(
                    storeName.getPos(),
                    RESOURCE.unknownStoreName( storeName.getSimple() ) );
        }
        long storeId = optStoreInstance.get().getAdapterId();
        // Check whether this placement already exists
        Optional<AllocationPlacement> optionalPlacement = statement.getTransaction().getSnapshot().alloc().getPlacement( storeId, table.id );
        if ( optionalPlacement.isEmpty() ) {
            throw CoreUtil.newContextException(
                    storeName.getPos(),
                    RESOURCE.placementDoesNotExist( storeName.getSimple(), table.name ) );
        }

        List<Long> partitionList = new ArrayList<>();

        // If index partitions are specified
        if ( !partitionGroups.isEmpty() && partitionGroupNames.isEmpty() ) {
            List<Long> partitionIds = partitions.stream().map( p -> p.id ).sorted().toList();
            //First convert specified index to correct partitionId
            for ( int partitionId : partitionGroups ) {
                // Check if specified partition index is even part of table and if so get corresponding uniquePartId
                try {
                    partitionList.add( partitionIds.get( partitionId ) );
                } catch ( IndexOutOfBoundsException e ) {
                    throw new GenericRuntimeException( "Specified Partition-Index: '%s' is not part of table '%s', has only %s partitions",
                            partitionId, table.name, partitions.size() );
                }
            }
        } else if ( !partitionGroupNames.isEmpty() && partitionGroups.isEmpty() ) {
            // If name partitions are specified
            // List<AllocationEntity> entities = catalog.getSnapshot().alloc().getAllocsOfPlacement( optionalPlacement.get().id );
            for ( String partitionName : partitionGroupNames.stream().map( Object::toString ).toList() ) {
                Optional<AllocationPartition> optionalPartition = partitions.stream().filter( p -> partitionName.equals( p.name ) ).findAny();

                if ( optionalPartition.isEmpty() ) {
                    throw new GenericRuntimeException( "There exists no partition with the identifier '%s'.", partitionName );
                }
                partitionList.add( optionalPartition.get().id );
            }
        }

        DdlManager.getInstance().modifyPartitionPlacement(
                table,
                partitionList,
                optStoreInstance.get(),
                statement
        );
    }

}

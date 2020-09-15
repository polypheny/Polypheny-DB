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

import org.polypheny.db.adapter.Store;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.*;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.*;
import org.polypheny.db.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.ImmutableNullableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.polypheny.db.util.Static.RESOURCE;


/**
 * Parse tree for {@code ALTER TABLE name MODIFY PARTITIONS (partitionId [, partitonId]* ) } statement.
 */
public class SqlAlterTableModifyPartitions extends SqlAlterTable  {


    private final SqlIdentifier table;
    private final SqlIdentifier storeName;
    List<Integer> partitionList;

    public SqlAlterTableModifyPartitions(SqlParserPos pos, SqlIdentifier table, SqlIdentifier storeName, List<Integer> partitionList) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.storeName = Objects.requireNonNull( storeName );
        this.partitionList = partitionList;
    }

    @Override
    public List<SqlNode> getOperandList() { return ImmutableNullableList.of( table, storeName ); }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec ) {
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
    public void execute(Context context, Statement statement) {
        Catalog catalog = Catalog.getInstance();
        CatalogTable catalogTable = getCatalogTable( context, table );
        if ( !catalogTable.isPartitioned ) {
            throw new RuntimeException("Table '" + catalogTable.name + "' is not partitioned");
        }

        try {
            //Check if table is already partitioned

                long tableId = catalogTable.id;


                if ( partitionList.isEmpty()){
                    throw new RuntimeException("Empty Partition Placement is not allowed for partitioned table '"+ catalogTable.name +"'");
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

                List<Long> tempPartitionList = new ArrayList<Long>();
                //First convert specified index to correct partitionId
                for (int partitionId: partitionList) {
                    //check if specified partition index is even part of table and if so get corresponding uniquePartId
                    try {
                        tempPartitionList.add(catalogTable.partitionIds.get(partitionId));
                    }catch (IndexOutOfBoundsException e){
                        throw new RuntimeException("Specified Partition-Index: '" + partitionId +"' is not part of table '"
                                + catalogTable.name+"', has only " + catalogTable.numPartitions + " partitions");
                    }
                }

                //Check if inmemory dataPartitonPlacement Map should even be chaneged and therefore start costly partitioning
                //Avoid unnecassary partitioning when the placement is already partitoined in the same way it has been specified
                if ( tempPartitionList.equals(catalog.getPartitionsOnDataPlacement(storeId, tableId)) ){
                    throw new RuntimeException("WARNING: The Data Placement for table: '" + catalogTable.name + "' on store: '"
                                    + storeName + "' already contains all specified partitions of statement: " + partitionList);
                }
                //Update
                catalog.updatePartitionsOnDataPlacement(storeId, tableId, tempPartitionList);

        } catch (UnknownTableException | UnknownStoreException e) {
            throw new RuntimeException( e );
        }
    }
}

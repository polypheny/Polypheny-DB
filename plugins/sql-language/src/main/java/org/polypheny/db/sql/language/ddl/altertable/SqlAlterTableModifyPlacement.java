/*
 * Copyright 2019-2023 The Polypheny Project
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


import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.ddl.SqlAlterTable;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name MODIFY PLACEMENT (columnList) ON STORE storeName} statement.
 */
@EqualsAndHashCode(callSuper = true)
@Slf4j
@Value
public class SqlAlterTableModifyPlacement extends SqlAlterTable {

    SqlIdentifier table;
    SqlNodeList addColumnList;
    SqlNodeList dropColumnList;
    SqlIdentifier storeName;
    List<Integer> partitionGroupList;
    List<SqlIdentifier> partitionGroupNamesList;


    public SqlAlterTableModifyPlacement(
            ParserPos pos,
            @NonNull SqlIdentifier table,
            SqlNodeList addColumnList,
            SqlNodeList dropColumnList,
            @NonNull SqlIdentifier storeName,
            List<Integer> partitionGroupList,
            List<SqlIdentifier> partitionGroupNamesList ) {
        super( pos );
        this.table = table;
        this.addColumnList = addColumnList == null ? SqlNodeList.EMPTY : addColumnList;
        this.dropColumnList = dropColumnList == null ? SqlNodeList.EMPTY : dropColumnList;
        this.storeName = Objects.requireNonNull( storeName );
        this.partitionGroupList = partitionGroupList;
        this.partitionGroupNamesList = partitionGroupNamesList;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( table, addColumnList, dropColumnList, storeName );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( table, addColumnList, dropColumnList, storeName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "MODIFY" );
        writer.keyword( "PLACEMENT" );
        writer.keyword( "ADD" );
        addColumnList.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "DROP" );
        dropColumnList.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ON" );
        writer.keyword( "STORE" );
        storeName.unparse( writer, leftPrec, rightPrec );

        if ( partitionGroupList != null || partitionGroupNamesList != null ) {
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
        LogicalTable table = getEntityFromCatalog( context, this.table );

        if ( table == null ) {
            throw new GenericRuntimeException( "Could not find the entity with name %s.", String.join( ".", this.table.names ) );
        }

        if ( table.entityType != EntityType.ENTITY ) {
            throw new GenericRuntimeException( "Not possible to use ALTER TABLE because " + table.name + " is not a table." );
        }

        // You can't partition placements if the table is not partitioned
        if ( !statement.getTransaction().getSnapshot().alloc().getPartitionProperty( table.id ).orElseThrow().isPartitioned && (!partitionGroupList.isEmpty() || !partitionGroupNamesList.isEmpty()) ) {
            throw new GenericRuntimeException( "Partition Placement is not allowed for un-partitioned table '" + table.name + "'" );
        }

        // Check if all columns exist
        for ( SqlNode node : Stream.concat( addColumnList.getSqlList().stream(), dropColumnList.getSqlList().stream() ).collect( Collectors.toList() ) ) {
            if ( getColumn( context, table.id, (SqlIdentifier) node ) == null ) {
                throw new GenericRuntimeException( "Could not find column with name %s", String.join( ".", ((SqlIdentifier) node).names ) );
            }
        }

        DataStore<?> store = getDataStoreInstance( storeName );
        DdlManager.getInstance().modifyPlacement(
                table,
                getColumns( context, table.id, addColumnList ).stream().map( c -> c.id ).collect( Collectors.toList() ),
                getColumns( context, table.id, dropColumnList ).stream().map( c -> c.id ).collect( Collectors.toList() ),
                partitionGroupList,
                partitionGroupNamesList.stream()
                        .map( SqlIdentifier::toString )
                        .collect( Collectors.toList() ),
                store,
                statement );

    }

}

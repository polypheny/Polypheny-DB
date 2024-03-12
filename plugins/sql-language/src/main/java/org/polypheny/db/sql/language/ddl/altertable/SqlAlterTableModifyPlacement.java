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


import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
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
    SqlNodeList columns;
    SqlIdentifier storeName;
    List<Integer> partitionGroups;
    List<SqlIdentifier> partitionGroupNames;


    public SqlAlterTableModifyPlacement(
            ParserPos pos,
            @NonNull SqlIdentifier table,
            @NonNull SqlNodeList columns,
            @NonNull SqlIdentifier storeName,
            List<Integer> partitionGroups,
            List<SqlIdentifier> partitionGroupNames ) {
        super( pos );
        this.table = table;
        this.columns = columns;
        this.storeName = Objects.requireNonNull( storeName );
        this.partitionGroups = partitionGroups;
        this.partitionGroupNames = partitionGroupNames;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( table, columns, storeName );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( table, columns, storeName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "MODIFY" );
        writer.keyword( "PLACEMENT" );
        columns.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ON" );
        writer.keyword( "STORE" );
        storeName.unparse( writer, leftPrec, rightPrec );

        if ( partitionGroups != null || partitionGroupNames != null ) {
            writer.keyword( " WITH " );
            writer.keyword( " PARTITIONS" );
            SqlWriter.Frame frame = writer.startList( "(", ")" );
            if ( partitionGroupNames != null ) {
                for ( int i = 0; i < partitionGroupNames.size(); i++ ) {
                    partitionGroupNames.get( i ).unparse( writer, leftPrec, rightPrec );
                    if ( i + 1 < partitionGroupNames.size() ) {
                        writer.sep( "," );
                    }
                }
            }
        }

    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        LogicalTable table = getTableFailOnEmpty( context, this.table );

        if ( table.entityType != EntityType.ENTITY ) {
            throw new GenericRuntimeException( "Not possible to use ALTER TABLE because " + table.name + " is not a table." );
        }

        // You can't partition placements if the table is not partitioned
        if ( !statement.getTransaction().getSnapshot().alloc().getPartitionProperty( table.id ).orElseThrow().isPartitioned && (!partitionGroups.isEmpty() || !partitionGroupNames.isEmpty()) ) {
            throw new GenericRuntimeException( "Partition Placement is not allowed for un-partitioned table '" + table.name + "'" );
        }

        // Check if all columns exist
        for ( SqlNode node : columns.getSqlList() ) {
            if ( getColumn( context, table.id, (SqlIdentifier) node ) == null ) {
                throw new GenericRuntimeException( "Could not find column with name %s", String.join( ".", ((SqlIdentifier) node).names ) );
            }
        }

        DataStore<?> store = getDataStoreInstance( storeName );

        DdlManager.getInstance().modifyPlacement(
                table,
                getColumns( context, table.id, columns ).stream().map( c -> c.id ).collect( Collectors.toList() ),
                partitionGroups,
                partitionGroupNames.stream()
                        .map( SqlIdentifier::toString )
                        .toList(),
                store,
                statement );

        if ( !partitionGroups.isEmpty() || !partitionGroupNames.isEmpty() ) {
            List<Long> ids = new ArrayList<>( partitionGroups.stream().map( id -> (long) id ).toList() );

            ids.addAll( partitionGroupNames.stream().map( SqlIdentifier::toString ).map( name -> Catalog.snapshot().alloc().getPartitionFromName( table.id, name ).orElseThrow().id ).toList() );

            DdlManager.getInstance().modifyPartitionPlacement(
                    table,
                    ids,
                    store,
                    statement
            );
        }

    }

}

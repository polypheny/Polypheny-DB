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
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionTypeException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.DdlManager.PartitionInformation;
import org.polypheny.db.ddl.exception.PartitionGroupNamesNotUniqueException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.partition.raw.RawPartitionInformation;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.sql.SqlIdentifier;
import org.polypheny.db.sql.sql.SqlNode;
import org.polypheny.db.sql.sql.SqlWriter;
import org.polypheny.db.sql.sql.ddl.SqlAlterTable;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name PARTITION BY partitionType (columnName) [PARTITIONS amount]} statement.
 */
@Slf4j
public class SqlAlterTableAddPartitions extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier partitionColumn;
    private final SqlIdentifier partitionType;
    private final int numPartitionGroups;
    private final int numPartitions;
    private final List<SqlIdentifier> partitionGroupNamesList;
    private final List<List<SqlNode>> partitionQualifierList;
    private final RawPartitionInformation rawPartitionInformation;


    public SqlAlterTableAddPartitions(
            ParserPos pos,
            SqlIdentifier table,
            SqlIdentifier partitionColumn,
            SqlIdentifier partitionType,
            int numPartitionGroups,
            int numPartitions,
            List<SqlIdentifier> partitionGroupNamesList,
            List<List<SqlNode>> partitionQualifierList,
            RawPartitionInformation rawPartitionInformation ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.partitionType = Objects.requireNonNull( partitionType );
        this.partitionColumn = Objects.requireNonNull( partitionColumn );
        this.numPartitionGroups = numPartitionGroups; //May be empty
        this.numPartitions = numPartitions; //May be empty
        this.partitionGroupNamesList = partitionGroupNamesList; //May be null and can only be used in association with PARTITION BY and PARTITIONS
        this.partitionQualifierList = partitionQualifierList;
        this.rawPartitionInformation = rawPartitionInformation;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( table, partitionType, partitionColumn );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( table, partitionType, partitionColumn );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "PARTITION" );
        writer.keyword( "BY" );
        partitionType.unparse( writer, leftPrec, rightPrec );

        switch ( partitionType.getSimple() ) {
            case "HASH":
                writer.keyword( "WITH" );
                SqlWriter.Frame frame = writer.startList( "(", ")" );
                for ( SqlIdentifier name : partitionGroupNamesList ) {
                    writer.sep( "," );
                    name.unparse( writer, 0, 0 );
                }
                break;
            case "RANGE":
            case "LIST":
                writer.keyword( "(" );
                for ( int i = 0; i < partitionGroupNamesList.size(); i++ ) {
                    writer.keyword( "PARTITION" );
                    partitionGroupNamesList.get( i ).unparse( writer, 0, 0 );
                    writer.keyword( "VALUES" );
                    writer.keyword( "(" );
                    partitionQualifierList.get( i ).get( 0 ).unparse( writer, 0, 0 );
                    writer.sep( "," );
                    partitionQualifierList.get( i ).get( 1 ).unparse( writer, 0, 0 );
                    writer.keyword( ")" );

                    if ( i < partitionGroupNamesList.size() ) {
                        writer.sep( "," );
                    }
                }
                writer.keyword( ")" );
                break;
        }
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        CatalogTable catalogTable = getCatalogTable( context, table );

        if ( catalogTable.tableType != TableType.TABLE ) {
            throw new RuntimeException( "Not possible to use ALTER TABLE because " + catalogTable.name + " is not a table." );
        }

        try {
            // Check if table is already partitioned
            if ( catalogTable.partitionProperty.partitionType == Catalog.PartitionType.NONE ) {
                DdlManager.getInstance().addPartitioning(
                        PartitionInformation.fromNodeLists(
                                catalogTable,
                                partitionType.getSimple(),
                                partitionColumn.getSimple(),
                                partitionGroupNamesList.stream().map( n -> (Identifier) n ).collect( Collectors.toList() ),
                                numPartitionGroups,
                                numPartitions,
                                partitionQualifierList.stream().map( l -> l.stream().map( e -> (Node) e ).collect( Collectors.toList() ) ).collect( Collectors.toList() ),
                                rawPartitionInformation ),
                        null,
                        statement );

            } else {
                throw new RuntimeException( "Table '" + catalogTable.name + "' is already partitioned" );
            }
        } catch ( UnknownPartitionTypeException | GenericCatalogException e ) {
            throw new RuntimeException( e );
        } catch ( PartitionGroupNamesNotUniqueException e ) {
            throw CoreUtil.newContextException( partitionColumn.getPos(), RESOURCE.partitionNamesNotUnique() );
        } catch ( UnknownColumnException e ) {
            throw CoreUtil.newContextException( partitionColumn.getPos(), RESOURCE.columnNotFoundInTable( partitionColumn.getSimple(), catalogTable.name ) );
        }
    }

}

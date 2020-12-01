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


import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionIdRuntimeException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionTypeException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.jdbc.Context;
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
    private final List< List<SqlNode>> partitionQualifierList;


    public SqlAlterTableAddPartitions(
            SqlParserPos pos,
            SqlIdentifier table,
            SqlIdentifier partitionColumn,
            SqlIdentifier partitionType,
            int numPartitions,
            List<SqlIdentifier> partitionNamesList,
            List< List<SqlNode>> partitionQualifierList ) {
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
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "PARTITION" );
        writer.keyword( "BY" );
        partitionType.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement ) {
        Catalog catalog = Catalog.getInstance();
        CatalogTable catalogTable = getCatalogTable( context, table );

        try {
            // Check if table is already partitioned
            if ( catalogTable.partitionType == Catalog.PartitionType.NONE ) {
                long tableId = catalogTable.id;

                // Check if specified partitionColumn is even part of the table
                Catalog.PartitionType actualPartitionType = Catalog.PartitionType.getByName( partitionType.toString() );

                long partitionColumnID = catalog.getColumn( tableId, partitionColumn.toString() ).id;
                if ( log.isDebugEnabled() ) {
                    log.debug( "Creating partition for table: {} with id {} on schema: {} on column: {}", catalogTable.name, catalogTable.id, catalogTable.getSchemaName(), partitionColumnID );
                }

                List<String> partitionValue = new ArrayList<>();
                List <List<String>> partitionQualifierStringList = new ArrayList<>();
                 for ( List<SqlNode> partitionValueList: partitionQualifierList) {
                    partitionQualifierStringList.add(partitionValueList.stream().map( Object::toString ).collect( Collectors.toList() ));
                }

                // TODO maybe create partitions multithreaded
                catalog.partitionTable(
                        tableId,
                        actualPartitionType,
                        partitionColumnID,
                        numPartitions,
                        //partitionQualifierList.stream().map( Object::toString ).collect( Collectors.toList() ),
                        partitionQualifierStringList,
                        partitionNamesList.stream().map( Object::toString ).collect( Collectors.toList() ) );

                if ( log.isDebugEnabled() ) {
                    log.debug( "Table: '{}' has been partitioned on columnId '{}'", catalogTable.name, catalogTable.columnIds.get( catalogTable.columnIds.indexOf( partitionColumnID ) ) );
                }
            } else {
                throw new RuntimeException( "Table '" + catalogTable.name + "' is already partitioned" );
            }
        } catch ( UnknownPartitionTypeException | UnknownColumnException | UnknownTableException | UnknownPartitionIdRuntimeException | GenericCatalogException e ) {
            throw new RuntimeException( e );
        }
    }
}

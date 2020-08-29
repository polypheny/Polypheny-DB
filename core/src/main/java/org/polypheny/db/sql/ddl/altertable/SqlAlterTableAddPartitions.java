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


import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.*;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

import static org.polypheny.db.util.Static.RESOURCE;


/**
 * Parse tree for {@code ALTER TABLE name PARTITION BY partitiontype (columnname) [PARTITIONS amount]} statement.
 */
public class SqlAlterTableAddPartitions extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier partitionColumn;
    private final SqlIdentifier partitionType;
    private final int numPartitions;

    public SqlAlterTableAddPartitions(SqlParserPos pos, SqlIdentifier table, SqlIdentifier partitionColumn, SqlIdentifier partitionType, int numPartitions) {
        super(pos);
        this.table = Objects.requireNonNull(table);
        this.partitionType= Objects.requireNonNull(partitionType);
        this.partitionColumn = Objects.requireNonNull(partitionColumn);
        this.numPartitions = numPartitions; //May be null

        System.out.println("HENNLO: " + table + "-" + partitionColumn + "-" + partitionType + "-" + numPartitions);
    }

    @Override
    public List<SqlNode> getOperandList() { return ImmutableNullableList.of( table, partitionType, partitionColumn ); }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "PARTITION" );
        writer.keyword( "BY" );
        partitionType.unparse( writer, leftPrec, rightPrec );

    }

    @Override
    public void execute(Context context, Transaction transaction) {
        Catalog catalog = Catalog.getInstance();
        CatalogTable catalogTable = getCatalogTable( context, table );

        try {
            //Check if table is already partitioned
            if ( catalogTable.partitionType == Catalog.PartitionType.NONE) {
                long tableId = catalogTable.id;

                //Check if specified partitionColumn is even part of the table
                Catalog.PartitionType actualPartitionType = actualPartitionType = Catalog.PartitionType.getByName(partitionType.toString());

                long partitionColumnID = catalog.getColumn(tableId,partitionColumn.toString()).id;
                System.out.println("HENNLO: SqlAlterTableAddPartition: execute(): Creating partition for table: " + catalogTable.name + " with id " + catalogTable.id +
                        " on schema: " + catalogTable.getSchemaName() + " on column: " + partitionColumnID);


                //TODO maybe create partitions multithreaded
                catalog.partitionTable(tableId, actualPartitionType, partitionColumnID, numPartitions);

                System.out.println("HENNLO: SqlAlterTableAddPartition: table: '" + catalogTable.name + "' has been partitioned on columnId '"
                        + catalogTable.columnIds.get(catalogTable.columnIds.indexOf(partitionColumnID)) +  "' ");
                //
            }
            else{
                throw new RuntimeException("Table '" + catalogTable.name + "' is already partitioned");
            }
        } catch (UnknownPartitionTypeException | UnknownColumnException  | UnknownTableException | UnknownPartitionException | GenericCatalogException e) {
            throw new RuntimeException( e );
        }
    }
}

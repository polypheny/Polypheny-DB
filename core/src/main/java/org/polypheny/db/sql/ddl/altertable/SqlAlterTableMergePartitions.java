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


import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name MERGE PARTITIONS} statement.
 */
@Slf4j
public class SqlAlterTableMergePartitions extends SqlAlterTable {

    private final SqlIdentifier table;


    public SqlAlterTableMergePartitions( SqlParserPos pos, SqlIdentifier table ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( table );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "MERGE" );
        writer.keyword( "PARTITIONS" );
    }


    @Override
    public void execute( Context context, Statement statement ) {
        Catalog catalog = Catalog.getInstance();
        CatalogTable catalogTable = getCatalogTable( context, table );

        // Check if table is even partitioned
        if ( catalogTable.partitionType != Catalog.PartitionType.NONE ) {
            long tableId = catalogTable.id;

            if ( log.isDebugEnabled() ) {
                log.debug( "Merging partitions for table: {} with id {} on schema: {}", catalogTable.name, catalogTable.id, catalogTable.getSchemaName() );
            }

            // TODO Create partitions multithreaded
            catalog.mergeTable( tableId );

            if ( log.isDebugEnabled() ) {
                log.debug( "Table: '{}' has been merged", catalogTable.name );
            }
        } else {
            throw new RuntimeException( "Table '" + catalogTable.name + "' is not partitioned at all" );
        }
    }

}

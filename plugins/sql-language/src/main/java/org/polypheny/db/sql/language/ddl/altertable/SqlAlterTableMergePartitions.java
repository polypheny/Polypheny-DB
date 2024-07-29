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


import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.PartitionType;
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
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name MERGE PARTITIONS} statement.
 */
@Slf4j
public class SqlAlterTableMergePartitions extends SqlAlterTable {

    private final SqlIdentifier table;


    public SqlAlterTableMergePartitions( ParserPos pos, SqlIdentifier table ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( table );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
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
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        LogicalTable table = getTableFailOnEmpty( context, this.table );

        if ( table.entityType != EntityType.ENTITY ) {
            throw new GenericRuntimeException( "Not possible to use ALTER TABLE because %s is not a table.", table.name );
        }

        // Check if table is even partitioned
        if ( statement.getTransaction().getSnapshot().alloc().getPartitionProperty( table.id ).orElseThrow().partitionType != PartitionType.NONE ) {

            if ( log.isDebugEnabled() ) {
                log.debug( "Merging partitions for table: {} with id {} on namespace: {}", table.name, table.id, statement.getTransaction().getSnapshot().getNamespace( table.namespaceId ).orElseThrow().name );
            }

            try {
                DdlManager.getInstance().dropTablePartition( table, statement );
            } catch ( TransactionException e ) {
                throw new GenericRuntimeException( "Error while merging partitions", e );
            }

            if ( log.isDebugEnabled() ) {
                log.debug( "Table: '{}' has been merged", table.name );
            }
        } else {
            throw new GenericRuntimeException( "Table '%s' is not partitioned!", table.name );
        }
    }

}

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

package org.polypheny.db.sql.language.ddl.altertable;


import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.EntityType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
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
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        CatalogTable catalogTable = getCatalogTable( context, table );

        if ( catalogTable.entityType != EntityType.ENTITY ) {
            throw new RuntimeException( "Not possible to use ALTER TABLE because " + catalogTable.name + " is not a table." );
        }

        // Check if table is even partitioned
        if ( catalogTable.partitionProperty.partitionType != Catalog.PartitionType.NONE ) {

            if ( log.isDebugEnabled() ) {
                log.debug( "Merging partitions for table: {} with id {} on schema: {}", catalogTable.name, catalogTable.id, catalogTable.getNamespaceName() );
            }

            try {
                DdlManager.getInstance().removePartitioning( catalogTable, statement );
            } catch ( UnknownDatabaseException | GenericCatalogException | UnknownTableException | TransactionException | UnknownSchemaException | UnknownUserException | UnknownKeyException e ) {
                throw new RuntimeException( "Error while merging partitions", e );
            }

            if ( log.isDebugEnabled() ) {
                log.debug( "Table: '{}' has been merged", catalogTable.name );
            }
        } else {
            throw new RuntimeException( "Table '" + catalogTable.name + "' is not partitioned!" );
        }
    }

}

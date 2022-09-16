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


import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.sql.SqlDataTypeSpec;
import org.polypheny.db.sql.sql.SqlIdentifier;
import org.polypheny.db.sql.sql.SqlNode;
import org.polypheny.db.sql.sql.SqlNodeList;
import org.polypheny.db.sql.sql.SqlWriter;
import org.polypheny.db.sql.sql.ddl.SqlAlterTable;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name MERGE COLUMNS name} statement.
 */
@Slf4j
public class SqlAlterTableMergeColumns extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlNodeList columnsToMerge;
    private final SqlIdentifier newColumnName; // Can be null

    private final SqlDataTypeSpec type;

    public SqlAlterTableMergeColumns(
            ParserPos pos,
            SqlIdentifier table,
            SqlNodeList columnsToMerge,
            SqlIdentifier newColumnName,
            SqlDataTypeSpec type ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.columnsToMerge = columnsToMerge;
        this.newColumnName = newColumnName;
        this.type = type;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( table, columnsToMerge );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( table, columnsToMerge );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "MERGE" );
        writer.keyword( "COLUMNS" );
        columnsToMerge.unparse( writer, leftPrec, rightPrec );
        if ( newColumnName != null ) {
            writer.keyword( "AFTER" );
            newColumnName.unparse( writer, leftPrec, rightPrec );
        }
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        CatalogTable catalogTable = getCatalogTable( context, table );

        if ( catalogTable.tableType != TableType.TABLE ) {
            throw new RuntimeException( "Not possible to use ALTER TABLE because " + catalogTable.name + " is not a table." );
        }

        /*
        if ( columnsToMerge.names.size() != 1 ) {
            throw new RuntimeException( "No FQDN allowed here: " + columnsToMerge.toString() );
        }
        */

        // Make sure that all adapters are of type store (and not source)
        for ( int storeId : catalogTable.dataPlacements ) {
            getDataStoreInstance( storeId );
        }

        /*
        try {
            DdlManager.getInstance().addColumn(
                    column.getSimple(),
                    catalogTable,
                    beforeColumnName == null ? null : beforeColumnName.getSimple(),
                    newColumnName == null ? null : newColumnName.getSimple(),
                    ColumnTypeInformation.fromDataTypeSpec( type ),
                    nullable,
                    defaultValue,
                    statement );


        } catch ( NotNullAndDefaultValueException e ) {
            throw CoreUtil.newContextException( column.getPos(), RESOURCE.notNullAndNoDefaultValue( column.getSimple() ) );
        } catch ( ColumnAlreadyExistsException e ) {
            throw CoreUtil.newContextException( column.getPos(), RESOURCE.columnExists( column.getSimple() ) );
        } catch ( ColumnNotExistsException e ) {
            throw CoreUtil.newContextException( table.getPos(), RESOURCE.columnNotFoundInTable( e.columnName, e.tableName ) );
        }

         */
    }

}


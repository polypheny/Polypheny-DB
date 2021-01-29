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


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DataSource.ExportedColumn;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name ADD COLUMN columnPhysical AS columnLogical} statement.
 */
@Slf4j
public class SqlAlterSourceTableAddColumn extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier columnPhysical;
    private final SqlIdentifier columnLogical;
    private final SqlNode defaultValue; // Can be null
    private final SqlIdentifier beforeColumnName; // Can be null
    private final SqlIdentifier afterColumnName; // Can be null


    public SqlAlterSourceTableAddColumn(
            SqlParserPos pos,
            SqlIdentifier table,
            SqlIdentifier columnPhysical,
            SqlIdentifier columnLogical,
            SqlNode defaultValue,
            SqlIdentifier beforeColumnName,
            SqlIdentifier afterColumnName ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.columnPhysical = Objects.requireNonNull( columnPhysical );
        this.columnLogical = Objects.requireNonNull( columnLogical );
        this.defaultValue = defaultValue;
        this.beforeColumnName = beforeColumnName;
        this.afterColumnName = afterColumnName;
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( table, columnPhysical, columnLogical );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ADD" );
        writer.keyword( "COLUMN" );
        columnPhysical.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "AS" );
        columnLogical.unparse( writer, leftPrec, rightPrec );
        if ( defaultValue != null ) {
            writer.keyword( "DEFAULT" );
            defaultValue.unparse( writer, leftPrec, rightPrec );
        }
        if ( beforeColumnName != null ) {
            writer.keyword( "BEFORE" );
            beforeColumnName.unparse( writer, leftPrec, rightPrec );
        } else if ( afterColumnName != null ) {
            writer.keyword( "AFTER" );
            afterColumnName.unparse( writer, leftPrec, rightPrec );
        }
    }


    @Override
    public void execute( Context context, Statement statement ) {
        Catalog catalog = Catalog.getInstance();
        CatalogTable catalogTable = getCatalogTable( context, table );

        if ( columnLogical.names.size() != 1 ) {
            throw new RuntimeException( "No FQDN allowed here: " + columnLogical.toString() );
        }

        CatalogColumn beforeColumn = null;
        if ( beforeColumnName != null ) {
            beforeColumn = getCatalogColumn( catalogTable.id, beforeColumnName );
        }
        CatalogColumn afterColumn = null;
        if ( afterColumnName != null ) {
            afterColumn = getCatalogColumn( catalogTable.id, afterColumnName );
        }

        if ( catalog.checkIfExistsColumn( catalogTable.id, columnLogical.getSimple() ) ) {
            throw SqlUtil.newContextException( columnLogical.getParserPosition(), RESOURCE.columnExists( columnLogical.getSimple() ) );
        }

        // Make sure that the table is of table type SOURCE
        if ( catalogTable.tableType != TableType.SOURCE ) {
            throw new RuntimeException( "Table '" + catalogTable.name + "' is not of type SOURCE!" );
        }

        // Make sure there is only one adapter
        if ( catalog.getColumnPlacements( catalogTable.columnIds.get( 0 ) ).size() != 1 ) {
            throw new RuntimeException( "The table has an unexpected number of placements!" );
        }

        int adapterId = catalog.getColumnPlacements( catalogTable.columnIds.get( 0 ) ).get( 0 ).adapterId;
        CatalogAdapter catalogAdapter = catalog.getAdapter( adapterId );
        DataSource dataSource = (DataSource) AdapterManager.getInstance().getAdapter( adapterId );

        String physicalTableName = catalog.getColumnPlacements( catalogTable.columnIds.get( 0 ) ).get( 0 ).physicalTableName;
        List<ExportedColumn> exportedColumns = dataSource.getExportedColumns().get( physicalTableName );

        // Check if physicalColumnName is valid
        ExportedColumn exportedColumn = null;
        for ( ExportedColumn ec : exportedColumns ) {
            if ( ec.physicalColumnName.equalsIgnoreCase( columnPhysical.getSimple() ) ) {
                exportedColumn = ec;
            }
        }
        if ( exportedColumn == null ) {
            throw new RuntimeException( "Invalid physical column name '" + columnPhysical.getSimple() + "'!" );
        }

        // Make sure this physical column has not already been added to this table
        for ( CatalogColumnPlacement ccp : catalog.getColumnPlacementsOnAdapter( adapterId, catalogTable.id ) ) {
            if ( ccp.physicalColumnName.equalsIgnoreCase( columnPhysical.getSimple() ) ) {
                throw new RuntimeException( "The physical column '" + columnPhysical.getSimple() + "' has already been added to this table!" );
            }
        }

        List<CatalogColumn> columns = catalog.getColumns( catalogTable.id );
        int position = columns.size() + 1;
        if ( beforeColumn != null || afterColumn != null ) {
            if ( beforeColumn != null ) {
                position = beforeColumn.position;
            } else {
                position = afterColumn.position + 1;
            }
            // Update position of the other columns
            for ( int i = columns.size(); i >= position; i-- ) {
                catalog.setColumnPosition( columns.get( i - 1 ).id, i + 1 );
            }
        }

        long columnId = catalog.addColumn(
                columnLogical.getSimple(),
                catalogTable.id,
                position,
                exportedColumn.type,
                exportedColumn.collectionsType,
                exportedColumn.length,
                exportedColumn.scale,
                exportedColumn.dimension,
                exportedColumn.cardinality,
                exportedColumn.nullable,
                Collation.CASE_INSENSITIVE
        );
        CatalogColumn addedColumn = catalog.getColumn( columnId );

        // Add default value
        if ( defaultValue != null ) {
            // TODO: String is only a temporal solution for default values
            String v = defaultValue.toString();
            if ( v.startsWith( "'" ) ) {
                v = v.substring( 1, v.length() - 1 );
            }
            catalog.setDefaultValue( addedColumn.id, PolyType.VARCHAR, v );

            // Update addedColumn variable
            addedColumn = catalog.getColumn( columnId );
        }

        // Add column placement
        catalog.addColumnPlacement(
                adapterId,
                addedColumn.id,
                PlacementType.STATIC,
                exportedColumn.physicalSchemaName,
                exportedColumn.physicalTableName,
                exportedColumn.physicalColumnName );

        // Rest plan cache and implementation cache (not sure if required in this case)
        statement.getQueryProcessor().resetCaches();
    }

}


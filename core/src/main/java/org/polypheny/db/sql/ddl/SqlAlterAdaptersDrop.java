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

package org.polypheny.db.sql.ddl;


import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.runtime.PolyphenyDbContextException;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.sql.SqlAlter;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.SqlSpecialOperator;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER ADAPTERS DROP uniqueName} statement.
 */
@Slf4j
public class SqlAlterAdaptersDrop extends SqlAlter {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "ALTER ADAPTERS DROP", SqlKind.OTHER_DDL );

    private final SqlNode uniqueName;


    /**
     * Creates a SqlAlterSchemaOwner.
     */
    public SqlAlterAdaptersDrop( SqlParserPos pos, SqlNode uniqueName ) {
        super( OPERATOR, pos );
        this.uniqueName = Objects.requireNonNull( uniqueName );
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( uniqueName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "ADAPTERS" );
        writer.keyword( "DROP" );
        uniqueName.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement ) {
        Catalog catalog = Catalog.getInstance();

        String uniqueNameStr = uniqueName.toString();
        if ( uniqueNameStr.startsWith( "'" ) ) {
            uniqueNameStr = uniqueNameStr.substring( 1 );
        }
        if ( uniqueNameStr.endsWith( "'" ) ) {
            uniqueNameStr = StringUtils.chop( uniqueNameStr );
        }
        
        try {
            CatalogAdapter catalogAdapter = Catalog.getInstance().getAdapter( uniqueNameStr );
            if ( catalogAdapter.type == AdapterType.SOURCE ) {
                Set<Long> tablesToDrop = new HashSet<>();
                for ( CatalogColumnPlacement ccp : catalog.getColumnPlacementsOnAdapter( catalogAdapter.id ) ) {
                    tablesToDrop.add( ccp.tableId );
                }
                for ( Long tableId : tablesToDrop ) {
                    CatalogTable table = catalog.getTable( tableId );

                    // Check if there are foreign keys referencing this table
                    List<CatalogForeignKey> selfRefsToDelete = new LinkedList<>();
                    try {
                        List<CatalogForeignKey> exportedKeys = catalog.getExportedKeys( tableId );
                        if ( exportedKeys.size() > 0 ) {
                            for ( CatalogForeignKey foreignKey : exportedKeys ) {
                                if ( foreignKey.tableId == tableId ) {
                                    // If this is a self-reference, drop it later.
                                    selfRefsToDelete.add( foreignKey );
                                } else {
                                    throw new PolyphenyDbException( "Cannot drop table '" + table.getSchemaName() + "." + table.name + "' because it is being referenced by '" + exportedKeys.get( 0 ).getSchemaName() + "." + exportedKeys.get( 0 ).getTableName() + "'." );
                                }
                            }

                        }
                    } catch ( GenericCatalogException e ) {
                        throw new PolyphenyDbContextException( "Exception while retrieving list of exported keys.", e );
                    }

                    // Make sure that there is only one adapter
                    if ( table.placementsByAdapter.keySet().size() != 1 ) {
                        throw new RuntimeException( "The data source contains tables with more than one placement. This schould not happen!" );
                    }

                    // Inform routing
                    statement.getRouter().dropPlacements( catalog.getColumnPlacementsOnAdapter( catalogAdapter.id, table.id ) );
                    // Delete column placement in catalog
                    for ( Long columnId : table.columnIds ) {
                        if ( catalog.checkIfExistsColumnPlacement( catalogAdapter.id, columnId ) ) {
                            catalog.deleteColumnPlacement( catalogAdapter.id, columnId );
                        }
                    }

                    // Delete keys and constraints
                    try {
                        // Remove primary key
                        catalog.deletePrimaryKey( table.id );
                    } catch ( GenericCatalogException e ) {
                        throw new PolyphenyDbContextException( "Exception while dropping primary key.", e );
                    }

                    // Delete columns
                    for ( Long columnId : table.columnIds ) {
                        catalog.deleteColumn( columnId );
                    }

                    // Delete the table
                    catalog.deleteTable( table.id );
                }

                // Rest plan cache and implementation cache
                statement.getQueryProcessor().resetCaches();
            }

            AdapterManager.getInstance().removeAdapter( catalogAdapter.id );
        } catch ( UnknownAdapterException e ) {
            throw new RuntimeException( "There is no adapter with this name: " + uniqueNameStr, e );
        } catch ( Exception e ) {
            throw new RuntimeException( "Could not remove data source " + uniqueNameStr, e );
        }
    }

}


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
import org.polypheny.db.UnknownTypeException;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownCollationException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlDataTypeSpec;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name DROP COLUMN name} statement.
 */
public class SqlAlterTableAddColumn extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier column;
    private final SqlDataTypeSpec type;
    private final boolean nullable;
    private final SqlNode defaultValue; // Can be null
    private final SqlIdentifier beforeColumnName; // Can be null
    private final SqlIdentifier afterColumnName; // Can be null


    public SqlAlterTableAddColumn(
            SqlParserPos pos,
            SqlIdentifier table,
            SqlIdentifier column,
            SqlDataTypeSpec type,
            boolean nullable,
            SqlNode defaultValue,
            SqlIdentifier beforeColumnName,
            SqlIdentifier afterColumnName ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.column = Objects.requireNonNull( column );
        this.type = Objects.requireNonNull( type );
        this.nullable = nullable;
        this.defaultValue = defaultValue;
        this.beforeColumnName = beforeColumnName;
        this.afterColumnName = afterColumnName;
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( table, column, type );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ADD" );
        writer.keyword( "COLUMN" );
        column.unparse( writer, leftPrec, rightPrec );
        type.unparse( writer, leftPrec, rightPrec );
        if ( nullable ) {
            writer.keyword( "NULL" );
        } else {
            writer.keyword( "NOT NULL" );
        }
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
    public void execute( Context context, Transaction transaction ) {
        CatalogCombinedTable catalogTable = getCatalogCombinedTable( context, transaction, table );

        if ( column.names.size() != 1 ) {
            throw new RuntimeException( "No FQDN allowed here: " + column.toString() );
        }

        CatalogColumn beforeColumn = null;
        if ( beforeColumnName != null ) {
            beforeColumn = getCatalogColumn( context, transaction, catalogTable.getTable().id, beforeColumnName );
        }
        CatalogColumn afterColumn = null;
        if ( afterColumnName != null ) {
            afterColumn = getCatalogColumn( context, transaction, catalogTable.getTable().id, afterColumnName );
        }

        // TODO: Check if the column either allows null values or has a default value defined.

        CatalogColumn addedColumn;
        try {
            if ( transaction.getCatalog().checkIfExistsColumn( catalogTable.getTable().id, column.getSimple() ) ) {
                throw SqlUtil.newContextException( column.getParserPosition(), RESOURCE.columnExists( column.getSimple() ) );
            }

            // Check whether all stores support schema changes
            for ( int storeId : catalogTable.getColumnPlacementsByStore().keySet() ) {
                if ( StoreManager.getInstance().getStore( storeId ).isSchemaReadOnly() ) {
                    throw SqlUtil.newContextException(
                            SqlParserPos.ZERO,
                            RESOURCE.storeIsSchemaReadOnly( StoreManager.getInstance().getStore( storeId ).getUniqueName() ) );
                }
            }

            List<CatalogColumn> columns = transaction.getCatalog().getColumns( catalogTable.getTable().id );
            int position = columns.size() + 1;
            if ( beforeColumn != null || afterColumn != null ) {
                if ( beforeColumn != null ) {
                    position = beforeColumn.position;
                } else {
                    position = afterColumn.position + 1;
                }
                // Update position of the other columns
                for ( int i = columns.size(); i >= position; i-- ) {
                    transaction.getCatalog().setColumnPosition( columns.get( i - 1 ).id, i + 1 );
                }
            }
            long columnId = transaction.getCatalog().addColumn(
                    column.getSimple(),
                    catalogTable.getTable().id,
                    position,
                    PolyType.get( type.getTypeName().getSimple() ),
                    type.getPrecision() == -1 ? null : type.getPrecision(),
                    type.getScale() == -1 ? null : type.getScale(),
                    nullable,
                    Collation.CASE_INSENSITIVE
            );
            addedColumn = transaction.getCatalog().getColumn( columnId );

            // Add default value
            if ( defaultValue != null ) {
                // TODO: String is only a temporal solution for default values
                String v = defaultValue.toString();
                if ( v.startsWith( "'" ) ) {
                    v = v.substring( 1, v.length() - 1 );
                }
                transaction.getCatalog().setDefaultValue( addedColumn.id, PolyType.VARCHAR, v );
            }

            // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            // TODO MV: Adding the column on all stores which have already a placement for columns of this table. We need a more sophisticated approach here.
            //

            // Add column on underlying data stores
            for ( int storeId : catalogTable.getColumnPlacementsByStore().keySet() ) {
                transaction.getCatalog().addColumnPlacement( storeId, addedColumn.id, PlacementType.AUTOMATIC, null, null, null );
                StoreManager.getInstance().getStore( storeId ).addColumn( context, catalogTable, addedColumn );
            }
        } catch ( GenericCatalogException | UnknownTypeException | UnknownCollationException | UnknownColumnException e ) {
            throw new RuntimeException( e );
        }

    }

}


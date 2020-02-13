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


import java.util.List;
import lombok.NonNull;
import org.polypheny.db.PolySqlType;
import org.polypheny.db.UnknownTypeException;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownCollationException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlDataTypeSpec;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name MODIFY COLUMN} statement.
 */
public class SqlAlterTableModifyColumn extends SqlAlterTable {

    private final SqlIdentifier tableName;
    private final SqlIdentifier columnName;

    private final SqlDataTypeSpec type;
    private final Boolean nullable;
    private final SqlIdentifier beforeColumn;
    private final SqlIdentifier afterColumn;
    private final SqlNode defaultValue;
    private final Boolean dropDefault;
    private final String collation;


    public SqlAlterTableModifyColumn(
            SqlParserPos pos,
            @NonNull SqlIdentifier tableName,
            @NonNull SqlIdentifier columnName,
            SqlDataTypeSpec type,
            Boolean nullable,
            SqlIdentifier beforeColumn,
            SqlIdentifier afterColumn,
            String collation,
            SqlNode defaultValue,
            Boolean dropDefault ) {
        super( pos );
        this.tableName = tableName;
        this.columnName = columnName;
        this.type = type;
        this.nullable = nullable;
        this.beforeColumn = beforeColumn;
        this.afterColumn = afterColumn;
        this.collation = collation;
        this.defaultValue = defaultValue;
        this.dropDefault = dropDefault;
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( tableName, columnName, type, beforeColumn, afterColumn, defaultValue );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        tableName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "MODIFY" );
        writer.keyword( "COLUMN" );
        columnName.unparse( writer, leftPrec, rightPrec );
        if ( type != null ) {
            writer.keyword( "SET" );
            writer.keyword( "TYPE" );
            type.unparse( writer, leftPrec, rightPrec );
        } else if ( nullable != null && !nullable ) {
            writer.keyword( "SET" );
            writer.keyword( "NOT" );
            writer.keyword( "NULL" );
        } else if ( nullable != null && nullable ) {
            writer.keyword( "SET" );
            writer.keyword( "NULL" );
        } else if ( beforeColumn != null ) {
            writer.keyword( "SET" );
            writer.keyword( "POSITION" );
            writer.keyword( "BEFORE" );
            beforeColumn.unparse( writer, leftPrec, rightPrec );
        } else if ( afterColumn != null ) {
            writer.keyword( "SET" );
            writer.keyword( "POSITION" );
            writer.keyword( "AFTER" );
            afterColumn.unparse( writer, leftPrec, rightPrec );
        } else if ( collation != null ) {
            writer.keyword( "SET" );
            writer.keyword( "COLLATION" );
            writer.literal( collation );
        } else if ( defaultValue != null ) {
            writer.keyword( "SET" );
            writer.keyword( "DEFAULT" );
            defaultValue.unparse( writer, leftPrec, rightPrec );
        } else if ( dropDefault != null && dropDefault ) {
            writer.keyword( "DROP" );
            writer.keyword( "DEFAULT" );
        } else {
            throw new RuntimeException( "Unknown option" );
        }
    }


    @Override
    public void execute( Context context, Transaction transaction ) {
        CatalogCombinedTable catalogTable = getCatalogCombinedTable( context, transaction, tableName );
        CatalogColumn catalogColumn = getCatalogColumn( context, transaction, catalogTable.getTable().id, columnName );

        try {
            if ( type != null ) {
                PolySqlType polySqlType = PolySqlType.getPolySqlTypeFromSting( type.getTypeName().getSimple() );
                transaction.getCatalog().setColumnType(
                        catalogColumn.id,
                        polySqlType,
                        type.getPrecision() == -1 ? null : type.getPrecision(),
                        type.getScale() == -1 ? null : type.getScale() );
                for ( int storeId : catalogTable.getColumnPlacementsByStore().keySet() ) {
                    StoreManager.getInstance().getStore( storeId ).updateColumnType( context, getCatalogColumn( context, transaction, catalogTable.getTable().id, columnName ) );
                }
            } else if ( nullable != null ) {
                transaction.getCatalog().setNullable( catalogColumn.id, nullable );
            } else if ( beforeColumn != null || afterColumn != null ) {
                int targetPosition;
                CatalogColumn refColumn;
                if ( beforeColumn != null ) {
                    refColumn = getCatalogColumn( context, transaction, catalogTable.getTable().id, beforeColumn );
                    targetPosition = refColumn.position;
                } else {
                    refColumn = getCatalogColumn( context, transaction, catalogTable.getTable().id, afterColumn );
                    targetPosition = refColumn.position + 1;
                }
                if ( catalogColumn.id == refColumn.id ) {
                    throw new RuntimeException( "Same column!" );
                }
                List<CatalogColumn> columns = transaction.getCatalog().getColumns( catalogTable.getTable().id );
                if ( targetPosition < catalogColumn.position ) {  // Walk from last column to first column
                    for ( int i = columns.size(); i >= 1; i-- ) {
                        if ( i < catalogColumn.position && i >= targetPosition ) {
                            transaction.getCatalog().setColumnPosition( columns.get( i - 1 ).id, i + 1 );
                        } else if ( i == catalogColumn.position ) {
                            transaction.getCatalog().setColumnPosition( catalogColumn.id, columns.size() + 1 );
                        }
                        if ( i == targetPosition ) {
                            transaction.getCatalog().setColumnPosition( catalogColumn.id, targetPosition );
                        }
                    }
                } else if ( targetPosition > catalogColumn.position ) { // Walk from first column to last column
                    targetPosition--;
                    for ( int i = 1; i <= columns.size(); i++ ) {
                        if ( i > catalogColumn.position && i <= targetPosition ) {
                            transaction.getCatalog().setColumnPosition( columns.get( i - 1 ).id, i - 1 );
                        } else if ( i == catalogColumn.position ) {
                            transaction.getCatalog().setColumnPosition( catalogColumn.id, columns.size() + 1 );
                        }
                        if ( i == targetPosition ) {
                            transaction.getCatalog().setColumnPosition( catalogColumn.id, targetPosition );
                        }
                    }
                } else {
                    // Do nothing
                }
            } else if ( collation != null ) {
                Collation col = Collation.parse( collation );
                transaction.getCatalog().setCollation( catalogColumn.id, col );
            } else if ( defaultValue != null ) {
                // TODO: String is only a temporal solution for default values
                String v = defaultValue.toString();
                if ( v.startsWith( "'" ) ) {
                    v = v.substring( 1, v.length() - 1 );
                }
                transaction.getCatalog().setDefaultValue( catalogColumn.id, PolySqlType.VARCHAR, v );
            } else if ( dropDefault != null && dropDefault ) {
                transaction.getCatalog().deleteDefaultValue( catalogColumn.id );
            } else {
                throw new RuntimeException( "Unknown option" );
            }
        } catch ( GenericCatalogException | UnknownTypeException | UnknownCollationException e ) {
            throw new RuntimeException( e );
        }
    }

}


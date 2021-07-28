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

package org.polypheny.cql.cql2rel;

import org.polypheny.cql.exception.InvalidMethodInvocation;
import org.polypheny.cql.exception.UnexpectedIndexFormatException;
import org.polypheny.cql.exception.UnknownIndexException;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;

public class Index {

    public final String schemaName;
    public final String tableName;
    public final String columnName;
    public final String fullyQualifiedName;

    private final CatalogTable catalogTable;
    private final CatalogColumn catalogColumn;
    private final boolean columnIndex;

    public Index( final CatalogTable catalogTable, final String schemaName, final String tableName ) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnName = null;
        this.fullyQualifiedName = schemaName + "." + tableName;
        this.columnIndex = false;

        this.catalogTable = catalogTable;
        this.catalogColumn = null;
    }


    public Index( final CatalogColumn catalogColumn, final String schemaName, final String tableName, final String columnName ) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.fullyQualifiedName = schemaName + "." + tableName + "." + columnName;
        this.columnIndex = true;

        this.catalogTable = null;
        this.catalogColumn = catalogColumn;
    }


    public static Index createIndex( String indexStr, String inDatabase )
            throws UnexpectedIndexFormatException, UnknownIndexException {

        String[] split = indexStr.split( "\\s" );
        if ( split.length != 1 ) {
            throw new UnexpectedIndexFormatException( "Index cannot contain whitespace: '" + indexStr + "'" );
        }

        String[] indexPieces = indexStr.split( "\\." );
        Catalog catalog = Catalog.getInstance();
        Index index;

        try {
            if ( indexPieces.length == 2 ) {
                CatalogTable table = catalog.getTable( inDatabase, indexPieces[0], indexPieces[1] );
                index = new Index( table, indexPieces[0], indexPieces[1] );
            } else if ( indexPieces.length == 3 ) {
                CatalogColumn column = catalog.getColumn( inDatabase, indexPieces[0], indexPieces[1], indexPieces[2] );
                index = new Index( column, indexPieces[0], indexPieces[1], indexPieces[2] );
            } else {
                throw new UnexpectedIndexFormatException( "Index must have atleast 1 and  atmost 2 periods: '" + indexStr + "'" );
            }
        } catch ( UnknownDatabaseException | UnknownTableException | UnknownSchemaException | UnknownColumnException e ) {
            String errorMessage = "Cannot find a underlying ";
            errorMessage += indexPieces.length == 3 ? "column" : "table";
            errorMessage += " for the specified index: " + indexStr;
            throw new UnknownIndexException( errorMessage );
        }

        return index;
    }


    public CatalogTable getCatalogTable() throws InvalidMethodInvocation {
        if ( isColumnIndex() ) {
            throw new InvalidMethodInvocation( "Invalid method call to get a CatalogTable from a ColumnIndex" );
        }
        return catalogTable;
    }


    public CatalogColumn getCatalogColumn() throws InvalidMethodInvocation {
        if ( isTableIndex() ) {
            throw new InvalidMethodInvocation( "Invalid method call to get a CatalogColumn from a TableIndex" );
        }
        return catalogColumn;
    }


    public boolean isTableIndex() {
        return !columnIndex;
    }


    public boolean isColumnIndex() {
        return columnIndex;
    }

}

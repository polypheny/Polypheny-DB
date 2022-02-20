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

package org.polypheny.db.sql.sql.ddl.altertable;


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import lombok.NonNull;
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownCollationException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.DdlManager.ColumnTypeInformation;
import org.polypheny.db.ddl.exception.ColumnNotExistsException;
import org.polypheny.db.ddl.exception.DdlOnSourceException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.sql.SqlDataTypeSpec;
import org.polypheny.db.sql.sql.SqlIdentifier;
import org.polypheny.db.sql.sql.SqlNode;
import org.polypheny.db.sql.sql.SqlWriter;
import org.polypheny.db.sql.sql.ddl.SqlAlterTable;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.CoreUtil;
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
            ParserPos pos,
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
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( tableName, columnName, type, beforeColumn, afterColumn, defaultValue );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
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
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        CatalogTable catalogTable = getCatalogTable( context, tableName );

        if ( catalogTable.tableType != TableType.TABLE ) {
            throw new RuntimeException( "Not possible to use ALTER TABLE because " + catalogTable.name + " is not a table." );
        }

        try {
            if ( type != null ) {
                DdlManager.getInstance().setColumnType( catalogTable, columnName.getSimple(), ColumnTypeInformation.fromDataTypeSpec( type ), statement );
            } else if ( nullable != null ) {
                DdlManager.getInstance().setColumnNullable( catalogTable, columnName.getSimple(), nullable, statement );
            } else if ( beforeColumn != null || afterColumn != null ) {
                DdlManager.getInstance().setColumnPosition( catalogTable, columnName.getSimple(), beforeColumn == null ? null : beforeColumn.getSimple(), afterColumn == null ? null : afterColumn.getSimple(), statement );
            } else if ( collation != null ) {
                DdlManager.getInstance().setColumnCollation( catalogTable, columnName.getSimple(), Collation.parse( collation ), statement );
            } else if ( defaultValue != null ) {
                DdlManager.getInstance().setDefaultValue( catalogTable, columnName.getSimple(), defaultValue.toString(), statement );
            } else if ( dropDefault != null && dropDefault ) {
                DdlManager.getInstance().dropDefaultValue( catalogTable, columnName.getSimple(), statement );
            } else {
                throw new RuntimeException( "Unknown option" );
            }


        } catch ( DdlOnSourceException e ) {
            throw CoreUtil.newContextException( tableName.getPos(), RESOURCE.ddlOnSourceTable() );
        } catch ( ColumnNotExistsException e ) {
            throw CoreUtil.newContextException( tableName.getPos(), RESOURCE.columnNotFoundInTable( e.columnName, e.tableName ) );
        } catch ( UnknownCollationException e ) {
            throw CoreUtil.newContextException( tableName.getPos(), RESOURCE.unknownCollation( collation ) );
        } catch ( GenericCatalogException e ) {
            throw new RuntimeException( e );
        }
    }

}


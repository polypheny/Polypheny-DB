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
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.ColumnAlreadyExistsException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.ColumnNotExistsException;
import org.polypheny.db.ddl.exception.DdlOnSourceException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.sql.SqlIdentifier;
import org.polypheny.db.sql.sql.SqlNode;
import org.polypheny.db.sql.sql.SqlWriter;
import org.polypheny.db.sql.sql.ddl.SqlAlterTable;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.CoreUtil;
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
            ParserPos pos,
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
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( table, columnPhysical, columnLogical );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
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
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        CatalogTable catalogTable = getCatalogTable( context, table );

        if ( catalogTable.tableType != TableType.SOURCE ) {
            throw new RuntimeException( "Not possible to use ALTER TABLE because " + catalogTable.name + " is not a source table." );
        }

        if ( columnLogical.names.size() != 1 ) {
            throw new RuntimeException( "No FQDN allowed here: " + columnLogical.toString() );
        }

        String defaultValue = this.defaultValue == null ? null : this.defaultValue.toString();

        try {
            DdlManager.getInstance().addColumnToSourceTable(
                    catalogTable,
                    columnPhysical.getSimple(),
                    columnLogical.getSimple(),
                    beforeColumnName == null ? null : beforeColumnName.getSimple(),
                    afterColumnName == null ? null : afterColumnName.getSimple(),
                    defaultValue,
                    statement );
        } catch ( ColumnAlreadyExistsException e ) {
            throw CoreUtil.newContextException( columnLogical.getPos(), RESOURCE.columnExists( columnLogical.getSimple() ) );
        } catch ( DdlOnSourceException e ) {
            throw CoreUtil.newContextException( table.getPos(), RESOURCE.ddlOnSourceTable() );
        } catch ( ColumnNotExistsException e ) {
            throw CoreUtil.newContextException( table.getPos(), RESOURCE.columnNotFoundInTable( e.columnName, e.tableName ) );
        }

    }

}


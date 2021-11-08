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

package org.polypheny.db.sql.ddl.altermaterializedview;

import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import java.util.Objects;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.DdlOnSourceException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterMaterializedView;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


public class SqlAlterMaterializedViewDropIndex extends SqlAlterMaterializedView {

    private final SqlIdentifier table;
    private final SqlIdentifier indexName;


    public SqlAlterMaterializedViewDropIndex( SqlParserPos pos, SqlIdentifier table, SqlIdentifier indexName ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.indexName = Objects.requireNonNull( indexName );
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( table, indexName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "MATERIALIZED VIEW" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "DROP" );
        writer.keyword( "INDEX" );
        indexName.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement ) {
        CatalogTable catalogTable = getCatalogTable( context, table );

        if ( catalogTable.tableType != TableType.MATERIALIZED_VIEW ) {
            throw new RuntimeException( "Not Possible to use ALTER MATERIALIZED VIEW because " + catalogTable.name + " is not a Materialized View." );
        }

        try {
            DdlManager.getInstance().dropIndex( catalogTable, indexName.getSimple(), statement );
        } catch ( DdlOnSourceException e ) {
            throw SqlUtil.newContextException( table.getParserPosition(), RESOURCE.ddlOnSourceTable() );
        }
    }

}

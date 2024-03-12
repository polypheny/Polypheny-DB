/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.sql.language.ddl.altermaterializedview;

import java.util.List;
import java.util.Objects;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.ddl.SqlAlterMaterializedView;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


public class SqlAlterMaterializedViewDropIndex extends SqlAlterMaterializedView {

    private final SqlIdentifier table;
    private final SqlIdentifier indexName;


    public SqlAlterMaterializedViewDropIndex( ParserPos pos, SqlIdentifier table, SqlIdentifier indexName ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.indexName = Objects.requireNonNull( indexName );
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( table, indexName );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
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
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        LogicalTable table = getTableFailOnEmpty( context, this.table );

        if ( table.entityType != EntityType.MATERIALIZED_VIEW ) {
            throw new GenericRuntimeException( "Not Possible to use ALTER MATERIALIZED VIEW because " + table.name + " is not a Materialized View." );
        }

        DdlManager.getInstance().dropIndex( table, indexName.getSimple(), statement );
    }

}

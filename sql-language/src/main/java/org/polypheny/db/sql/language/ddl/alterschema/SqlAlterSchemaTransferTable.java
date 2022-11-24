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

package org.polypheny.db.sql.language.ddl.alterschema;


import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.EntityAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.ddl.SqlAlterSchema;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

import static org.polypheny.db.util.Static.RESOURCE;


/**
 * Parse tree for {@code ALTER SCHEMA name OWNER TO} statement.
 */
public class SqlAlterSchemaTransferTable extends SqlAlterSchema {

    private final SqlIdentifier table;
    private final SqlIdentifier targetSchema;


    /**
     * Creates a SqlAlterSchemaOwner.
     */
    public SqlAlterSchemaTransferTable(ParserPos pos, SqlIdentifier table, SqlIdentifier targetSchema) {
        super( pos );
        this.table = Objects.requireNonNull(table);
        this.targetSchema = Objects.requireNonNull(targetSchema);
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of(table, targetSchema);
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of(table, targetSchema);
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "SCHEMA" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "OWNER" );
        writer.keyword( "TO" );
        targetSchema.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        try {
            Catalog catalog = Catalog.getInstance();
            CatalogTable catalogTable = getCatalogTable( context, table);

            long targetSchemaId = catalog.getSchema( context.getDatabaseId(), targetSchema.getNames().get(0) ).id;
            DdlManager.getInstance().transferTable( catalogTable, targetSchemaId, statement );

        } catch ( UnknownSchemaException e ) {
            throw CoreUtil.newContextException( table.getPos(), RESOURCE.schemaNotFound( table.getSimple() ) );
        } catch (EntityAlreadyExistsException e) {
            throw CoreUtil.newContextException( table.getPos(), RESOURCE.tableExists(  table.names.get( 1 ) ) );
        }
    }

}

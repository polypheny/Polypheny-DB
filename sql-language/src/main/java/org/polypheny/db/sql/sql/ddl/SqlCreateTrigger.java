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

package org.polypheny.db.sql.sql.ddl;


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.sql.*;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.CoreUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.polypheny.db.util.Static.RESOURCE;


/**
 * Parse tree for {@code CREATE TRIGGER} statement.
 */
public class SqlCreateTrigger extends SqlCreate implements ExecutableStatement {

    private final SqlIdentifier name;
    private final SqlNode query;
    private final SqlIdentifier table;
    private final SqlIdentifier schema;
    private final String event;
    private final boolean notForReplication;

    private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator( "CREATE TRIGGER", Kind.CREATE_TRIGGER );

    /**
     * Creates a SqlCreateFunction.
     */
    public SqlCreateTrigger(ParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier schema, SqlIdentifier name, SqlIdentifier table, String event, boolean notForReplication, SqlNode query) {
        super( OPERATOR, pos, replace, ifNotExists );
        this.name = Objects.requireNonNull( name );
        this.query = Objects.requireNonNull( query );
        this.table = table;
        this.schema = schema;
        this.event = event;
        this.notForReplication = notForReplication;
    }

    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( getReplace() ? "CREATE OR REPLACE" : "CREATE" );
        writer.keyword( "TRIGGER" );
        if ( ifNotExists ) {
            writer.keyword( "IF NOT EXISTS" );
        }
        schema.unparse( writer, 0, 0 );
        writer.keyword(".");
        name.unparse( writer, 0, 0 );
        writer.keyword("ON");
        table.unparse( writer, 0, 0 );
        writer.keyword("AFTER");
        writer.keyword(event.toUpperCase());
        writer.keyword("$");
        query.unparse(writer, 0, 0);
    }

    @Override
    public Operator getOperator() {
        return OPERATOR;
    }


    @Override
    public List<Node> getOperandList() {
        return Arrays.asList( name, query);
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return Arrays.asList( name, query);
    }

    @Override
    public void execute(Context context, Statement statement, QueryParameters parameters) {

    }
}

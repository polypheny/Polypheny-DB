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


import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
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
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.polypheny.db.util.Static.RESOURCE;


/**
 * Parse tree for {@code CREATE PROCEDURE} statement.
 */
public class SqlCreateProcedure extends SqlCreate implements ExecutableStatement {

    private final SqlIdentifier name;
    private final SqlNode query;

    private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator( "CREATE PROCEDURE", Kind.CREATE_PROCEDURE );


    /**
     * Creates a SqlCreateFunction.
     */
    public SqlCreateProcedure(ParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name, SqlNode query) {
        super( OPERATOR, pos, replace, ifNotExists );
        this.name = Objects.requireNonNull( name );
        this.query = Objects.requireNonNull( query );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( getReplace() ? "CREATE OR REPLACE" : "CREATE" );
        writer.keyword( "PROCEDURE" );
        if ( ifNotExists ) {
            writer.keyword( "IF NOT EXISTS" );
        }
        name.unparse( writer, 0, 0 );
        // parse ParamList
/*        if ( argumentList.size() > 0 ) {
            final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.SIMPLE );
            for ( SqlNode argument : argumentList.getSqlList() ) {
                writer.sep( "," );
                writer.keyword("@");
                argument.unparse( writer, 0, 0 );
            }
            writer.endList( frame );
        }*/
        writer.keyword( "AS" );
        // parse query
        query.unparse( writer, 0, 0 );
        writer.keyword("GO");
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
        DdlManager instance = DdlManager.getInstance();

        Catalog catalog = Catalog.getInstance();
        long schemaId;
        String procedureName;
        long databaseId = context.getDatabaseId();
        try {
            if ( name.names.size() == 3 ) { // DatabaseName.SchemaName.ProcedureName
                schemaId = catalog.getSchema( name.names.get( 0 ), name.names.get( 1 ) ).id;
                procedureName = name.names.get( 2 );
                databaseId = catalog.getDatabase(name.names.get(0)).id;
            } else if ( name.names.size() == 2 ) { // SchemaName.ProcedureName
                schemaId = catalog.getSchema( databaseId, name.names.get( 0 ) ).id;
                procedureName = name.names.get( 1 );
            } else { // ProcedureName
                schemaId = catalog.getSchema( context.getDatabaseId(), context.getDefaultSchemaName() ).id;
                procedureName = name.names.get(0);
            }
        } catch ( UnknownDatabaseException e ) {
            throw CoreUtil.newContextException( name.getPos(), RESOURCE.databaseNotFound( name.toString() ) );
        } catch ( UnknownSchemaException e ) {
            throw CoreUtil.newContextException( name.getPos(), RESOURCE.schemaNotFound( name.toString() ) );
        }

        try {
            // TODO refactor Procedure to use proper syntax to extract params and query
            instance.createProcedure(schemaId, name.getSimple(), databaseId, replace, "");
        } catch (GenericCatalogException | UnknownColumnException e) {
            e.printStackTrace();
        }
    }
}

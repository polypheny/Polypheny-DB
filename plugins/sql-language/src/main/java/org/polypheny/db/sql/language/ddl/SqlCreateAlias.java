/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.sql.language.ddl;


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.language.*;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

import static org.polypheny.db.util.Static.RESOURCE;


/**
 * Parse tree for {@code CREATE ALIAS} statement.
 */
public class SqlCreateAlias extends SqlCreate implements ExecutableStatement {

    private final SqlIdentifier name;
    private final SqlIdentifier alias;

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "CREATE ALIAS", Kind.CREATE_ALIAS );


    /**
     * Creates a SqlCreateType.
     */
    SqlCreateAlias(ParserPos pos, boolean replace, SqlIdentifier name, SqlIdentifier alias ) {
        super( OPERATOR, pos, replace, false );
        this.name = Objects.requireNonNull( name );
        this.alias = Objects.requireNonNull( alias );
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        Catalog catalog = Catalog.getInstance();
        String tableName;
        String aliasName;
        long schemaId;

        try {
            // Cannot use getTable() here since table does not yet exist
            if ( name.names.size() == 3 ) { // DatabaseName.SchemaName.TableName
                schemaId = catalog.getSchema( name.names.get( 0 ), name.names.get( 1 ) ).id;
                tableName = name.names.get( 2 );
            } else if ( name.names.size() == 2 ) { // SchemaName.TableName
                schemaId = catalog.getSchema( context.getDatabaseId(), name.names.get( 0 ) ).id;
                tableName = name.names.get( 1 );
            } else { // TableName
                schemaId = catalog.getSchema( context.getDatabaseId(), context.getDefaultSchemaName() ).id;
                tableName = name.names.get( 0 );
            }
        } catch ( UnknownDatabaseException e ) {
            throw CoreUtil.newContextException( name.getPos(), RESOURCE.databaseNotFound( name.toString() ) );
        } catch ( UnknownSchemaException e ) {
            throw CoreUtil.newContextException( name.getPos(), RESOURCE.schemaNotFound( name.toString() ) );
        }

        if ( alias.names.size() != 1 ) {
            throw new RuntimeException("Currently only a simple alias name is allowed");
        }

        aliasName = alias.names.get(0);

        if(!replace && catalog.isAlias(aliasName)) {
            throw new RuntimeException("Alias name " + aliasName + " already exists");
        }

        Object[] value = new Object[]{ catalog.getSchema(schemaId).databaseId, schemaId, tableName };
        catalog.addAlias(aliasName, value);

    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( name, alias );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( name, alias );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        if ( getReplace() ) {
            writer.keyword( "CREATE OR REPLACE" );
        } else {
            writer.keyword( "CREATE" );
        }
        writer.keyword( "ALIAS" );
        name.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "AS" );
        alias.unparse( writer, 0, 0 );
    }

}


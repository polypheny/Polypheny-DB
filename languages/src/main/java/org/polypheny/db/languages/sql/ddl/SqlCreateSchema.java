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

package org.polypheny.db.languages.sql.ddl;


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import java.util.Objects;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.exceptions.SchemaAlreadyExistsException;
import org.polypheny.db.core.Kind;
import org.polypheny.db.core.ParserPos;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.languages.sql.SqlCreate;
import org.polypheny.db.languages.sql.SqlExecutableStatement;
import org.polypheny.db.languages.sql.SqlIdentifier;
import org.polypheny.db.languages.sql.SqlOperator;
import org.polypheny.db.languages.sql.SqlSpecialOperator;
import org.polypheny.db.languages.sql.SqlUtil;
import org.polypheny.db.languages.sql.SqlWriter;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code CREATE SCHEMA} statement.
 */
public class SqlCreateSchema extends SqlCreate implements SqlExecutableStatement {

    private final SqlIdentifier name;

    private final SchemaType type;

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "CREATE SCHEMA", Kind.CREATE_SCHEMA );


    /**
     * Creates a SqlCreateSchema.
     */
    SqlCreateSchema( ParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name, SchemaType schemaType ) {
        super( OPERATOR, pos, replace, ifNotExists );
        this.name = Objects.requireNonNull( name );
        this.type = schemaType;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( name );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "CREATE" );
        if ( replace ) {
            writer.keyword( "OR REPLACE" );
        }
        writer.keyword( "SCHEMA" );
        if ( ifNotExists ) {
            writer.keyword( "IF NOT EXISTS" );
        }
        name.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement ) {
        try {
            DdlManager.getInstance().createSchema( name.getSimple(), context.getDatabaseId(), type, context.getCurrentUserId(), ifNotExists, replace );
        } catch ( SchemaAlreadyExistsException e ) {
            throw SqlUtil.newContextException( name.getPos(), RESOURCE.schemaExists( name.getSimple() ) );
        }

    }

}

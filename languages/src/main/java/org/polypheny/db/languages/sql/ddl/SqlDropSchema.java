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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.DdlOnSourceException;
import org.polypheny.db.ddl.exception.SchemaNotExistException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.languages.sql.SqlDrop;
import org.polypheny.db.languages.sql.SqlExecutableStatement;
import org.polypheny.db.languages.sql.SqlIdentifier;
import org.polypheny.db.core.Kind;
import org.polypheny.db.languages.sql.SqlNode;
import org.polypheny.db.languages.sql.SqlOperator;
import org.polypheny.db.languages.sql.SqlSpecialOperator;
import org.polypheny.db.languages.sql.SqlUtil;
import org.polypheny.db.languages.sql.SqlWriter;
import org.polypheny.db.core.ParserPos;
import org.polypheny.db.transaction.Statement;


/**
 * Parse tree for {@code DROP SCHEMA} statement.
 */
public class SqlDropSchema extends SqlDrop implements SqlExecutableStatement {

    private final SqlIdentifier name;

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "DROP SCHEMA", Kind.DROP_TABLE );


    /**
     * Creates a SqlDropSchema.
     */
    SqlDropSchema( ParserPos pos, boolean ifExists, SqlIdentifier name ) {
        super( OPERATOR, pos, ifExists );
        this.name = name;
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of( name );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "DROP SCHEMA" );
        if ( ifExists ) {
            writer.keyword( "IF EXISTS" );
        }
        name.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement ) {
        try {
            DdlManager.getInstance().dropSchema( context.getDatabaseId(), name.getSimple(), ifExists, statement );
        } catch ( SchemaNotExistException e ) {
            throw SqlUtil.newContextException( name.getPos(), RESOURCE.schemaNotFound( name.getSimple() ) );
        } catch ( DdlOnSourceException e ) {
            throw SqlUtil.newContextException( name.getPos(), RESOURCE.ddlOnSourceTable() );
        }
    }

}

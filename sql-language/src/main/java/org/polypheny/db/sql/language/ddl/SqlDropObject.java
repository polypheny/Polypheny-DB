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

package org.polypheny.db.sql.language.ddl;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.sql.language.SqlDrop;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlWriter;


/**
 * Base class for parse trees of {@code DROP TABLE}, {@code DROP VIEW}, and {@code DROP TYPE} statements.
 */
abstract class SqlDropObject extends SqlDrop implements ExecutableStatement {

    protected final SqlIdentifier name;


    /**
     * Creates a SqlDropObject.
     */
    SqlDropObject( SqlOperator operator, ParserPos pos, boolean ifExists, SqlIdentifier name ) {
        super( operator, pos, ifExists );
        this.name = name;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableList.of( name );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableList.of( name );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( getOperator().getName() ); // "DROP TABLE" etc.
        if ( ifExists ) {
            writer.keyword( "IF EXISTS" );
        }
        name.unparse( writer, leftPrec, rightPrec );
    }


    /*
    public void execute( Context context ) {
        final List<String> path = context.getDefaultSchemaPath();
        PolyphenyDbSchema schema = context.getRootSchema();
        for ( String p : path ) {
            schema = schema.getSubSchema( p, true );
        }
        final boolean existed;
        switch ( getKind() ) {
            case DROP_TABLE:
            case DROP_MATERIALIZED_VIEW:
                existed = schema.removeTable( name.getSimple() );
                if ( !existed && !ifExists ) {
                    throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.tableNotFound( name.getSimple() ) );
                }
                break;
            case DROP_VIEW:
                // Not quite right: removes any other functions with the same name
                existed = schema.removeFunction( name.getSimple() );
                if ( !existed && !ifExists ) {
                    throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.viewNotFound( name.getSimple() ) );
                }
                break;
            case DROP_TYPE:
                existed = schema.removeType( name.getSimple() );
                if ( !existed && !ifExists ) {
                    throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.typeNotFound( name.getSimple() ) );
                }
                break;
            case OTHER_DDL:
            default:
                throw new AssertionError( getKind() );
        }
    }*/
}

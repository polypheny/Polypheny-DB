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


import java.util.List;
import java.util.Objects;
import org.polypheny.db.core.ExecutableStatement;
import org.polypheny.db.core.enums.Kind;
import org.polypheny.db.core.nodes.Node;
import org.polypheny.db.core.ParserPos;
import org.polypheny.db.core.QueryParameters;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.languages.sql.SqlCreate;
import org.polypheny.db.languages.sql.SqlDataTypeSpec;
import org.polypheny.db.languages.sql.SqlIdentifier;
import org.polypheny.db.languages.sql.SqlNode;
import org.polypheny.db.languages.sql.SqlNodeList;
import org.polypheny.db.languages.sql.SqlOperator;
import org.polypheny.db.languages.sql.SqlSpecialOperator;
import org.polypheny.db.languages.sql.SqlWriter;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code CREATE TYPE} statement.
 */
public class SqlCreateType extends SqlCreate implements ExecutableStatement {

    private final SqlIdentifier name;
    private final SqlNodeList attributeDefs;
    private final SqlDataTypeSpec dataType;

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "CREATE TYPE", Kind.CREATE_TYPE );


    /**
     * Creates a SqlCreateType.
     */
    SqlCreateType( ParserPos pos, boolean replace, SqlIdentifier name, SqlNodeList attributeDefs, SqlDataTypeSpec dataType ) {
        super( OPERATOR, pos, replace, false );
        this.name = Objects.requireNonNull( name );
        this.attributeDefs = attributeDefs; // may be null
        this.dataType = dataType; // may be null
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        DdlManager.getInstance().createType();
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( name, attributeDefs );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( name, attributeDefs );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        if ( getReplace() ) {
            writer.keyword( "CREATE OR REPLACE" );
        } else {
            writer.keyword( "CREATE" );
        }
        writer.keyword( "TYPE" );
        name.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "AS" );
        if ( attributeDefs != null ) {
            SqlWriter.Frame frame = writer.startList( "(", ")" );
            for ( SqlNode a : attributeDefs.getSqlList() ) {
                writer.sep( "," );
                a.unparse( writer, 0, 0 );
            }
            writer.endList( frame );
        } else if ( dataType != null ) {
            dataType.unparse( writer, leftPrec, rightPrec );
        }
    }

/*
    @Override
    public void execute( Context context ) {
        final Pair<PolyphenyDbSchema, String> pair = SqlDdlNodes.schema( context, true, name );
        pair.left.add( pair.right, typeFactory -> {
            if ( dataType != null ) {
                return dataType.deriveType( typeFactory );
            } else {
                final RelDataTypeFactory.Builder builder = typeFactory.builder();
                for ( SqlNode def : attributeDefs ) {
                    final SqlAttributeDefinition attributeDef = (SqlAttributeDefinition) def;
                    final SqlDataTypeSpec typeSpec = attributeDef.dataType;
                    RelDataType type = typeSpec.deriveType( typeFactory );
                    if ( type == null ) {
                        Pair<PolyphenyDbSchema, String> pair1 = SqlDdlNodes.schema( context, false, typeSpec.getTypeName() );
                        type = pair1.left.getType( pair1.right, false ).getType().apply( typeFactory );
                    }
                    builder.add( attributeDef.name.getSimple(), type );
                }
                return builder.build();
            }
        } );
    } */
}


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


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlCollation;
import org.polypheny.db.sql.language.SqlDataTypeSpec;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.sql.language.SqlWriter;


/**
 * Parse tree for SqlAttributeDefinition, which is part of a {@link SqlCreateType}.
 */
public class SqlAttributeDefinition extends SqlCall {

    private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator( "ATTRIBUTE_DEF", Kind.ATTRIBUTE_DEF );

    final SqlIdentifier name;
    final SqlDataTypeSpec dataType;
    final SqlNode expression;
    final SqlCollation collation;


    /**
     * Creates a SqlAttributeDefinition; use {@link SqlDdlNodes#attribute}.
     */
    SqlAttributeDefinition( ParserPos pos, SqlIdentifier name, SqlDataTypeSpec dataType, SqlNode expression, SqlCollation collation ) {
        super( pos );
        this.name = name;
        this.dataType = dataType;
        this.expression = expression;
        this.collation = collation;
    }


    @Override
    public Operator getOperator() {
        return OPERATOR;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableList.of( name, dataType );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableList.of( name, dataType );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        name.unparse( writer, 0, 0 );
        dataType.unparse( writer, 0, 0 );
        if ( collation != null ) {
            writer.keyword( "COLLATE" );
            collation.unparse( writer, 0, 0 );
        }
        if ( dataType.getNullable() != null && !dataType.getNullable() ) {
            writer.keyword( "NOT NULL" );
        }
        if ( expression != null ) {
            writer.keyword( "DEFAULT" );
            exp( writer );
        }
    }


    // TODO: refactor this to a util class to share with SqlColumnDeclaration
    private void exp( SqlWriter writer ) {
        if ( writer.isAlwaysUseParentheses() ) {
            expression.unparse( writer, 0, 0 );
        } else {
            writer.sep( "(" );
            expression.unparse( writer, 0, 0 );
            writer.sep( ")" );
        }
    }

}

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

package org.polypheny.db.sql.language;


import java.util.List;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * A <code>SqlDescribeSchema</code> is a node of a parse tree that represents a {@code DESCRIBE SCHEMA} statement.
 */
public class SqlDescribeSchema extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator( "DESCRIBE_SCHEMA", Kind.DESCRIBE_SCHEMA ) {
                @Override
                public Call createCall( Literal functionQualifier, ParserPos pos, Node... operands ) {
                    return new SqlDescribeSchema( pos, (SqlIdentifier) operands[0] );
                }
            };

    SqlIdentifier schema;


    /**
     * Creates a SqlDescribeSchema.
     */
    public SqlDescribeSchema( ParserPos pos, SqlIdentifier schema ) {
        super( pos );
        this.schema = schema;
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "DESCRIBE" );
        writer.keyword( "SCHEMA" );
        schema.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void setOperand( int i, Node operand ) {
        switch ( i ) {
            case 0:
                schema = (SqlIdentifier) operand;
                break;
            default:
                throw new AssertionError( i );
        }
    }


    @Override
    public Operator getOperator() {
        return OPERATOR;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( schema );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( schema );
    }


    public SqlIdentifier getSchema() {
        return schema;
    }

}

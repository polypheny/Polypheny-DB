/*
 * Copyright 2019-2024 The Polypheny Project
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
 * A <code>SqlDescribeNamespace</code> is a node of a parse tree that represents a {@code DESCRIBE NAMESPACE} statement.
 */
public class SqlDescribeNamespace extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator( "DESCRIBE_NAMESPACE", Kind.DESCRIBE_NAMESPACE ) {
                @Override
                public Call createCall( Literal functionQualifier, ParserPos pos, Node... operands ) {
                    return new SqlDescribeNamespace( pos, (SqlIdentifier) operands[0] );
                }
            };

    SqlIdentifier namespace;


    /**
     * Creates a SqlDescribeNamespace.
     */
    public SqlDescribeNamespace( ParserPos pos, SqlIdentifier namespace ) {
        super( pos );
        this.namespace = namespace;
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "DESCRIBE" );
        writer.keyword( "NAMESPACE" );
        namespace.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void setOperand( int i, Node operand ) {
        switch ( i ) {
            case 0:
                namespace = (SqlIdentifier) operand;
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
        return ImmutableNullableList.of( namespace );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( namespace );
    }


    public SqlIdentifier getNamespace() {
        return namespace;
    }

}

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
 * An item in a WITH clause of a query.
 * It has a name, an optional column list, and a query.
 */
public class SqlWithItem extends SqlCall {

    public SqlIdentifier name;
    public SqlNodeList columnList; // may be null
    public SqlNode query;


    public SqlWithItem( ParserPos pos, SqlIdentifier name, SqlNodeList columnList, SqlNode query ) {
        super( pos );
        this.name = name;
        this.columnList = columnList;
        this.query = query;
    }


    @Override
    public Kind getKind() {
        return Kind.WITH_ITEM;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( name, columnList, query );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( name, columnList, query );
    }


    @Override
    public void setOperand( int i, Node operand ) {
        switch ( i ) {
            case 0:
                name = (SqlIdentifier) operand;
                break;
            case 1:
                columnList = (SqlNodeList) operand;
                break;
            case 2:
                query = (SqlNode) operand;
                break;
            default:
                throw new AssertionError( i );
        }
    }


    @Override
    public Operator getOperator() {
        return SqlWithItemOperator.INSTANCE;
    }


    /**
     * SqlWithItemOperator is used to represent an item in a WITH clause of a query. It has a name, an optional column list, and a query.
     */
    private static class SqlWithItemOperator extends SqlSpecialOperator {

        private static final SqlWithItemOperator INSTANCE = new SqlWithItemOperator();


        SqlWithItemOperator() {
            super( "WITH_ITEM", Kind.WITH_ITEM, 0 );
        }


        @Override
        public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
            final SqlWithItem withItem = (SqlWithItem) call;
            withItem.name.unparse( writer, getLeftPrec(), getRightPrec() );
            if ( withItem.columnList != null ) {
                withItem.columnList.unparse( writer, getLeftPrec(), getRightPrec() );
            }
            writer.keyword( "AS" );
            withItem.query.unparse( writer, 10, 10 );
        }


        @Override
        public Call createCall( Literal functionQualifier, ParserPos pos, Node... operands ) {
            assert functionQualifier == null;
            assert operands.length == 3;
            return new SqlWithItem( pos, (SqlIdentifier) operands[0], (SqlNodeList) operands[1], (SqlNode) operands[2] );
        }

    }

}


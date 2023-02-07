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


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;


/**
 * The WITH clause of a query. It wraps a SELECT, UNION, or INTERSECT.
 */
public class SqlWith extends SqlCall {

    public SqlNodeList withList;
    public SqlNode body;


    public SqlWith( ParserPos pos, SqlNodeList withList, SqlNode body ) {
        super( pos );
        this.withList = withList;
        this.body = body;
    }


    @Override
    public Kind getKind() {
        return Kind.WITH;
    }


    @Override
    public Operator getOperator() {
        return SqlWithOperator.INSTANCE;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableList.of( withList, body );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableList.of( withList, body );
    }


    @Override
    public void setOperand( int i, Node operand ) {
        switch ( i ) {
            case 0:
                withList = (SqlNodeList) operand;
                break;
            case 1:
                body = (SqlNode) operand;
                break;
            default:
                throw new AssertionError( i );
        }
    }


    @Override
    public void validate( SqlValidator validator, SqlValidatorScope scope ) {
        validator.validateWith( this, scope );
    }


    /**
     * SqlWithOperator is used to represent a WITH clause of a query. It wraps a SELECT, UNION, or INTERSECT.
     */
    private static class SqlWithOperator extends SqlSpecialOperator {

        private static final SqlWithOperator INSTANCE = new SqlWithOperator();


        private SqlWithOperator() {
            // NOTE:  make precedence lower then SELECT to avoid extra parens
            super( "WITH", Kind.WITH, 2 );
        }


        @Override
        public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
            final SqlWith with = (SqlWith) call;
            final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.WITH, "WITH", "" );
            final SqlWriter.Frame frame1 = writer.startList( "", "" );
            for ( SqlNode node : with.withList.getSqlList() ) {
                writer.sep( "," );
                node.unparse( writer, 0, 0 );
            }
            writer.endList( frame1 );
            final SqlWriter.Frame frame2 = writer.startList( SqlWriter.FrameTypeEnum.SIMPLE );
            with.body.unparse( writer, 100, 100 );
            writer.endList( frame2 );
            writer.endList( frame );
        }


        @Override
        public SqlCall createCall( Literal functionQualifier, ParserPos pos, Node... operands ) {
            return new SqlWith( pos, (SqlNodeList) operands[0], (SqlNode) operands[1] );
        }


        @Override
        public void validateCall( SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope ) {
            validator.validateWith( (SqlWith) call, scope );
        }

    }

}


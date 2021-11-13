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

package org.polypheny.db.languages.sql;


import java.util.List;
import org.polypheny.db.core.Kind;
import org.polypheny.db.core.Node;
import org.polypheny.db.core.ParserPos;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * A <code>SqlExplain</code> is a node of a parse tree which represents an EXPLAIN PLAN statement.
 */
public class SqlExplain extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator( "EXPLAIN", Kind.EXPLAIN ) {
                @Override
                public SqlCall createCall( SqlLiteral functionQualifier, ParserPos pos, SqlNode... operands ) {
                    return new SqlExplain( pos, operands[0], (SqlLiteral) operands[1], (SqlLiteral) operands[2], (SqlLiteral) operands[3], 0 );
                }
            };


    /**
     * The level of abstraction with which to display the plan.
     */
    public enum Depth {
        TYPE, LOGICAL, PHYSICAL;


        /**
         * Creates a parse-tree node representing an occurrence of this symbol at a particular position in the parsed text.
         */
        public SqlLiteral symbol( ParserPos pos ) {
            return SqlLiteral.createSymbol( this, pos );
        }
    }


    SqlNode explicandum;
    SqlLiteral detailLevel;
    SqlLiteral depth;
    SqlLiteral format;
    private final int dynamicParameterCount;


    public SqlExplain( ParserPos pos, SqlNode explicandum, SqlLiteral detailLevel, SqlLiteral depth, SqlLiteral format, int dynamicParameterCount ) {
        super( pos );
        this.explicandum = explicandum;
        this.detailLevel = detailLevel;
        this.depth = depth;
        this.format = format;
        this.dynamicParameterCount = dynamicParameterCount;
    }


    @Override
    public Kind getKind() {
        return Kind.EXPLAIN;
    }


    @Override
    public Operator getOperator() {
        return OPERATOR;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( explicandum, detailLevel, depth, format );
    }


    @Override
    public void setOperand( int i, Node operand ) {
        switch ( i ) {
            case 0:
                explicandum = operand;
                break;
            case 1:
                detailLevel = (SqlLiteral) operand;
                break;
            case 2:
                depth = (SqlLiteral) operand;
                break;
            case 3:
                format = (SqlLiteral) operand;
                break;
            default:
                throw new AssertionError( i );
        }
    }


    /**
     * @return the underlying SQL statement to be explained
     */
    public SqlNode getExplicandum() {
        return explicandum;
    }


    /**
     * @return detail level to be generated
     */
    public SqlExplainLevel getDetailLevel() {
        return detailLevel.symbolValue( SqlExplainLevel.class );
    }


    /**
     * Returns the level of abstraction at which this plan should be displayed.
     */
    public Depth getDepth() {
        return depth.symbolValue( Depth.class );
    }


    /**
     * @return the number of dynamic parameters in the statement
     */
    public int getDynamicParamCount() {
        return dynamicParameterCount;
    }


    /**
     * @return whether physical plan implementation should be returned
     */
    public boolean withImplementation() {
        return getDepth() == Depth.PHYSICAL;
    }


    /**
     * @return whether type should be returned
     */
    public boolean withType() {
        return getDepth() == Depth.TYPE;
    }


    /**
     * Returns the desired output format.
     */
    public SqlExplainFormat getFormat() {
        return format.symbolValue( SqlExplainFormat.class );
    }


    /**
     * Returns whether result is to be in JSON format.
     */
    public boolean isJson() {
        return getFormat() == SqlExplainFormat.XML;
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "EXPLAIN PLAN" );
        switch ( getDetailLevel() ) {
            case NO_ATTRIBUTES:
                writer.keyword( "EXCLUDING ATTRIBUTES" );
                break;
            case EXPPLAN_ATTRIBUTES:
                writer.keyword( "INCLUDING ATTRIBUTES" );
                break;
            case ALL_ATTRIBUTES:
                writer.keyword( "INCLUDING ALL ATTRIBUTES" );
                break;
        }
        switch ( getDepth() ) {
            case TYPE:
                writer.keyword( "WITH TYPE" );
                break;
            case LOGICAL:
                writer.keyword( "WITHOUT IMPLEMENTATION" );
                break;
            case PHYSICAL:
                writer.keyword( "WITH IMPLEMENTATION" );
                break;
            default:
                throw new UnsupportedOperationException();
        }
        switch ( getFormat() ) {
            case XML:
                writer.keyword( "AS XML" );
                break;
            case JSON:
                writer.keyword( "AS JSON" );
                break;
            default:
        }
        writer.keyword( "FOR" );
        writer.newlineAndIndent();
        explicandum.unparse( writer, getOperator().getLeftPrec(), getOperator().getRightPrec() );
    }
}


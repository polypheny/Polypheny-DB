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
import java.util.Objects;
import lombok.Getter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.util.UnmodifiableArrayList;


/**
 * Implementation of {@link SqlCall} that keeps its operands in an array.
 */
public class SqlBasicCall extends SqlCall {

    private SqlOperator operator;
    @Getter
    public final SqlNode[] operands;
    private final SqlLiteral functionQuantifier;
    private final boolean expanded;


    public SqlBasicCall( SqlOperator operator, SqlNode[] operands, ParserPos pos ) {
        this( operator, operands, pos, false, null );
    }


    protected SqlBasicCall( SqlOperator operator, SqlNode[] operands, ParserPos pos, boolean expanded, SqlLiteral functionQualifier ) {
        super( pos );
        this.operator = Objects.requireNonNull( operator );
        this.operands = operands;
        this.expanded = expanded;
        this.functionQuantifier = functionQualifier;
    }


    @Override
    public Kind getKind() {
        return operator.getKind();
    }


    @Override
    public boolean isExpanded() {
        return expanded;
    }


    @Override
    public void setOperand( int i, Node operand ) {
        operands[i] = (SqlNode) operand;
    }


    public void setOperator( SqlOperator operator ) {
        this.operator = Objects.requireNonNull( operator );
    }


    @Override
    public Operator getOperator() {
        return operator;
    }


    @Override
    public List<Node> getOperandList() {
        return UnmodifiableArrayList.of( operands ); // not immutable, but quick
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return UnmodifiableArrayList.of( operands ); // not immutable, but quick
    }


    @SuppressWarnings("unchecked")
    @Override
    public <S extends Node> S operand( int i ) {
        return (S) operands[i];
    }


    @Override
    public int operandCount() {
        return operands.length;
    }


    @Override
    public SqlLiteral getFunctionQuantifier() {
        return functionQuantifier;
    }


    @Override
    public Call clone( ParserPos pos ) {
        return getOperator().createCall( getFunctionQuantifier(), pos, operands );
    }

}


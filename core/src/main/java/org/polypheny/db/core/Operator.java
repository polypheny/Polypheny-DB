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

package org.polypheny.db.core;

import java.util.List;
import org.polypheny.db.core.BasicNodeVisitor.ArgHandler;
import org.polypheny.db.core.Function.FunctionType;
import org.polypheny.db.core.operators.OperatorName;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.util.Litmus;

public interface Operator {

    String NL = System.getProperty( "line.separator" );

    FunctionType getFunctionType();

    Syntax getSyntax();

    Call createCall( Literal functionQualifier, ParserPos pos, Node... operands );

    RelDataType inferReturnType( OperatorBinding opBinding );

    RelDataType deriveType( Validator validator, ValidatorScope scope, Call call );

    RelDataType inferReturnType( RelDataTypeFactory typeFactory, List<RelDataType> operandTypes );

    abstract Kind getKind();

    abstract String getName();

    boolean validRexOperands( int count, Litmus litmus );

    Call createCall( ParserPos pos, Node... operands );

    Call createCall( NodeList nodeList );

    Call createCall( ParserPos pos, List<? extends Node> operandList );

    String getAllowedSignatures();

    String getAllowedSignatures( String opNameToUse );

    <R> R acceptCall( NodeVisitor<R> visitor, Call call );

    <R> void acceptCall( NodeVisitor<R> visitor, Call call, boolean onlyExpressions, ArgHandler<R> argHandler );

    boolean isAggregator();

    boolean requiresOver();

    boolean requiresOrder();

    boolean isGroup();

    boolean isGroupAuxiliary();

    boolean isDeterministic();

    boolean isDynamicFunction();

    boolean requiresDecimalExpansion();

    String getSignatureTemplate( int operandsCount );

    Monotonicity getMonotonicity( OperatorBinding call );

    OperatorName getOperatorName();

    void setOperatorName( OperatorName name );

}

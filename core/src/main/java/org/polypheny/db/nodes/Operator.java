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

package org.polypheny.db.nodes;

import java.util.List;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.constant.Syntax;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.BasicNodeVisitor.ArgHandler;
import org.polypheny.db.nodes.Function.FunctionType;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.util.Litmus;

public interface Operator {

    String NL = System.getProperty( "line.separator" );

    FunctionType getFunctionType();

    Syntax getSyntax();

    Call createCall( Literal functionQualifier, ParserPos pos, Node... operands );

    AlgDataType inferReturnType( OperatorBinding opBinding );

    AlgDataType deriveType( Validator validator, ValidatorScope scope, Call call );

    AlgDataType inferReturnType( AlgDataTypeFactory typeFactory, List<AlgDataType> operandTypes );

    Kind getKind();

    String getName();

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

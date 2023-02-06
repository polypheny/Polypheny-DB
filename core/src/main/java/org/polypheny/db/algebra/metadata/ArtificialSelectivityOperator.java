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

package org.polypheny.db.algebra.metadata;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.constant.Syntax;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.BasicNodeVisitor.ArgHandler;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Function.FunctionType;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeList;
import org.polypheny.db.nodes.NodeVisitor;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.Litmus;

public class ArtificialSelectivityOperator implements Operator {

    @Getter
    FunctionCategory functionCategory = FunctionCategory.SYSTEM;


    @Override
    public FunctionType getFunctionType() {
        return null;
    }


    @Override
    public Syntax getSyntax() {
        return null;
    }


    @Override
    public Call createCall( Literal functionQualifier, ParserPos pos, Node... operands ) {
        return null;
    }


    @Override
    public AlgDataType inferReturnType( OperatorBinding opBinding ) {
        return new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT ).createPolyType( PolyType.BOOLEAN );
    }


    @Override
    public AlgDataType deriveType( Validator validator, ValidatorScope scope, Call call ) {
        return null;
    }


    @Override
    public AlgDataType inferReturnType( AlgDataTypeFactory typeFactory, List<AlgDataType> operandTypes ) {
        return null;
    }


    @Override
    public Kind getKind() {
        return Kind.OTHER_FUNCTION;
    }


    @Override
    public String getName() {
        return "ARTIFICIAL_SELECTIVITY";
    }


    @Override
    public boolean validRexOperands( int count, Litmus litmus ) {
        return true;
    }


    @Override
    public Call createCall( ParserPos pos, Node... operands ) {
        return null;
    }


    @Override
    public Call createCall( NodeList nodeList ) {
        return null;
    }


    @Override
    public Call createCall( ParserPos pos, List<? extends Node> operandList ) {
        return null;
    }


    @Override
    public String getAllowedSignatures() {
        return null;
    }


    @Override
    public String getAllowedSignatures( String opNameToUse ) {
        return null;
    }


    @Override
    public <R> R acceptCall( NodeVisitor<R> visitor, Call call ) {
        return null;
    }


    @Override
    public <R> void acceptCall( NodeVisitor<R> visitor, Call call, boolean onlyExpressions, ArgHandler<R> argHandler ) {

    }


    @Override
    public boolean isAggregator() {
        return false;
    }


    @Override
    public boolean requiresOver() {
        return false;
    }


    @Override
    public boolean requiresOrder() {
        return false;
    }


    @Override
    public boolean isGroup() {
        return false;
    }


    @Override
    public boolean isGroupAuxiliary() {
        return false;
    }


    @Override
    public boolean isDeterministic() {
        return false;
    }


    @Override
    public boolean isDynamicFunction() {
        return false;
    }


    @Override
    public boolean requiresDecimalExpansion() {
        return false;
    }


    @Override
    public String getSignatureTemplate( int operandsCount ) {
        return null;
    }


    @Override
    public Monotonicity getMonotonicity( OperatorBinding call ) {
        return null;
    }


    @Override
    public OperatorName getOperatorName() {
        return null;
    }


    @Override
    public void setOperatorName( OperatorName name ) {

    }

}

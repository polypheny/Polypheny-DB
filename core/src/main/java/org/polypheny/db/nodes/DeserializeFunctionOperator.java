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

package org.polypheny.db.nodes;

import java.util.List;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Syntax;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.type.PolyType;


public class DeserializeFunctionOperator extends OperatorImpl {

    private final static AlgDataTypeFactory factory = new JavaTypeFactoryImpl();
    private final static AlgDataType mapType = factory
            .createMapType( factory.createPolyType( PolyType.ANY ), factory.createPolyType( PolyType.ANY ) );


    public DeserializeFunctionOperator( String name ) {
        super( name, Kind.DESERIALIZE, null, null, null );
    }


    @Override
    public Syntax getSyntax() {
        return Syntax.SPECIAL;
    }


    @Override
    public Call createCall( Literal functionQualifier, ParserPos pos, Node... operands ) {
        return null;
    }


    @Override
    public AlgDataType inferReturnType( OperatorBinding opBinding ) {
        return mapType;
    }


    @Override
    public AlgDataType deriveType( Validator validator, ValidatorScope scope, Call call ) {
        return null;
    }


    @Override
    public AlgDataType inferReturnType( AlgDataTypeFactory typeFactory, List<AlgDataType> operandTypes ) {
        return mapType;
    }

}

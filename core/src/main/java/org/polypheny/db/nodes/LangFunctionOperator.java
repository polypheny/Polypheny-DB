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

package org.polypheny.db.nodes;

import java.util.List;
import javax.annotation.Nullable;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Syntax;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.type.PolyType;

public class LangFunctionOperator extends OperatorImpl {

    private final PolyType returnType;
    private final @Nullable PolyType returnComponentType;


    public LangFunctionOperator( String name, Kind kind, PolyType returnType, @Nullable PolyType returnComponentType ) {
        super( name, kind, null, null, null );
        this.returnType = returnType;
        this.returnComponentType = returnComponentType;
    }


    public LangFunctionOperator( String name, Kind kind, PolyType returnType ) {
        this( name, kind, returnType, null );
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
        return getReturnType();
    }


    private AlgDataType getReturnType() {
        if ( returnComponentType != null ) {
            return AlgDataTypeFactory.DEFAULT.createArrayType( AlgDataTypeFactory.DEFAULT.createPolyType( returnComponentType ), -1 );
        }

        return switch ( returnType ) {
            case VARCHAR -> AlgDataTypeFactory.DEFAULT.createPolyType( returnType, 2050 );
            default -> AlgDataTypeFactory.DEFAULT.createPolyType( returnType );
        };
    }


    @Override
    public AlgDataType deriveType( Validator validator, ValidatorScope scope, Call call ) {
        throw new GenericRuntimeException( "Not Implemented" );
    }


    @Override
    public AlgDataType inferReturnType( AlgDataTypeFactory typeFactory, List<AlgDataType> operandTypes ) {
        return getReturnType();
    }

}

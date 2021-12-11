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

package org.polypheny.db.nodes;


import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.validate.ValidatorException;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.runtime.Resources;
import org.polypheny.db.util.CoreUtil;


/**
 * <code>ExplicitOperatorBinding</code> implements {@link OperatorBinding} via an underlying array of known operand types.
 */
public class ExplicitOperatorBinding extends OperatorBinding {

    private final List<AlgDataType> types;
    private final OperatorBinding delegate;


    public ExplicitOperatorBinding( OperatorBinding delegate, List<AlgDataType> types ) {
        this(
                delegate,
                delegate.getTypeFactory(),
                delegate.getOperator(),
                types );
    }


    public ExplicitOperatorBinding( AlgDataTypeFactory typeFactory, Operator operator, List<AlgDataType> types ) {
        this( null, typeFactory, operator, types );
    }


    private ExplicitOperatorBinding( OperatorBinding delegate, AlgDataTypeFactory typeFactory, Operator operator, List<AlgDataType> types ) {
        super( typeFactory, operator );
        this.types = types;
        this.delegate = delegate;
    }


    // implement SqlOperatorBinding
    @Override
    public int getOperandCount() {
        return types.size();
    }


    // implement SqlOperatorBinding
    @Override
    public AlgDataType getOperandType( int ordinal ) {
        return types.get( ordinal );
    }


    @Override
    public PolyphenyDbException newError( Resources.ExInst<ValidatorException> e ) {
        if ( delegate != null ) {
            return delegate.newError( e );
        } else {
            return CoreUtil.newContextException( ParserPos.ZERO, e );
        }
    }


    @Override
    public boolean isOperandNull( int ordinal, boolean allowCast ) {
        // NOTE jvs 1-May-2006:  This call is only relevant for SQL validation, so anywhere else, just say everything's OK.
        return false;
    }

}



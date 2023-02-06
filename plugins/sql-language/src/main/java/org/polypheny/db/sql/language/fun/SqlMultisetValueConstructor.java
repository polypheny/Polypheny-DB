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

package org.polypheny.db.sql.language.fun;


import java.util.List;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlCallBinding;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.InferTypes;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.Static;


/**
 * Definition of the SQL:2003 standard MULTISET constructor, <code>MULTISET [&lt;expr&gt;, ...]</code>.
 *
 * Derived classes construct other kinds of collections.
 *
 * @see SqlMultisetQueryConstructor
 */
public class SqlMultisetValueConstructor extends SqlSpecialOperator {


    public SqlMultisetValueConstructor() {
        this( "MULTISET", Kind.MULTISET_VALUE_CONSTRUCTOR );
    }


    protected SqlMultisetValueConstructor( String name, Kind kind ) {
        super(
                name,
                kind, MDX_PRECEDENCE,
                false,
                ReturnTypes.ARG0,
                InferTypes.FIRST_KNOWN,
                OperandTypes.VARIADIC );
    }


    @Override
    public AlgDataType inferReturnType( OperatorBinding opBinding ) {
        AlgDataType type =
                getComponentType(
                        opBinding.getTypeFactory(),
                        opBinding.collectOperandTypes() );
        if ( null == type ) {
            return null;
        }
        return PolyTypeUtil.createMultisetType(
                opBinding.getTypeFactory(),
                type,
                false );
    }


    protected AlgDataType getComponentType( AlgDataTypeFactory typeFactory, List<AlgDataType> argTypes ) {
        return typeFactory.leastRestrictive( argTypes );
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        final List<AlgDataType> argTypes =
                PolyTypeUtil.deriveAndCollectTypes(
                        callBinding.getValidator(),
                        callBinding.getScope(),
                        callBinding.operands() );
        if ( argTypes.size() == 0 ) {
            throw callBinding.newValidationError( Static.RESOURCE.requireAtLeastOneArg() );
        }
        final AlgDataType componentType = getComponentType( callBinding.getTypeFactory(), argTypes );
        if ( null == componentType ) {
            if ( throwOnFailure ) {
                throw callBinding.newValidationError( Static.RESOURCE.needSameTypeParameter() );
            }
            return false;
        }
        return true;
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        writer.keyword( getName() ); // "MULTISET" or "ARRAY"
        final SqlWriter.Frame frame = writer.startList( "[", "]" );
        for ( SqlNode operand : call.getSqlOperandList() ) {
            writer.sep( "," );
            operand.unparse( writer, leftPrec, rightPrec );
        }
        writer.endList( frame );
    }

}


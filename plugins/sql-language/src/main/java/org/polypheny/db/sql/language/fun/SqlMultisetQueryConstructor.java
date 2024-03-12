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

package org.polypheny.db.sql.language.fun;


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlCallBinding;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlSelect;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorNamespace;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * Definition of the SQL:2003 standard MULTISET query constructor, <code>MULTISET (&lt;query&gt;)</code>.
 *
 * @see SqlMultisetValueConstructor
 */
public class SqlMultisetQueryConstructor extends SqlSpecialOperator {

    public SqlMultisetQueryConstructor() {
        this( "MULTISET", Kind.MULTISET_QUERY_CONSTRUCTOR );
    }


    protected SqlMultisetQueryConstructor( String name, Kind kind ) {
        super(
                name,
                kind, MDX_PRECEDENCE,
                false,
                ReturnTypes.ARG0,
                null,
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


    private AlgDataType getComponentType( AlgDataTypeFactory typeFactory, List<AlgDataType> argTypes ) {
        return typeFactory.leastRestrictive( argTypes );
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        final List<AlgDataType> argTypes =
                PolyTypeUtil.deriveAndCollectTypes(
                        callBinding.getValidator(),
                        callBinding.getScope(),
                        callBinding.operands() );
        final AlgDataType componentType =
                getComponentType(
                        callBinding.getTypeFactory(),
                        argTypes );
        if ( null == componentType ) {
            if ( throwOnFailure ) {
                throw callBinding.newValidationError( RESOURCE.needSameTypeParameter() );
            }
            return false;
        }
        return true;
    }


    @Override
    public AlgDataType deriveType( Validator validator, ValidatorScope scope, Call call ) {
        SqlSelect subSelect = call.operand( 0 );
        subSelect.validateExpr( (SqlValidator) validator, (SqlValidatorScope) scope );
        SqlValidatorNamespace ns = ((SqlValidator) validator).getSqlNamespace( subSelect );
        assert null != ns.getTupleType();
        return PolyTypeUtil.createMultisetType(
                validator.getTypeFactory(),
                ns.getTupleType(),
                false );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        writer.keyword( getName() );
        final SqlWriter.Frame frame = writer.startList( "(", ")" );
        assert call.operandCount() == 1;
        ((SqlNode) call.operand( 0 )).unparse( writer, leftPrec, rightPrec );
        writer.endList( frame );
    }


    @Override
    public boolean argumentMustBeScalar( int ordinal ) {
        return false;
    }

}


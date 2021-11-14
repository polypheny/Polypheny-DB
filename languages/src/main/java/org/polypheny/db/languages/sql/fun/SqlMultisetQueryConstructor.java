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

package org.polypheny.db.languages.sql.fun;


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import org.polypheny.db.core.Kind;
import org.polypheny.db.languages.sql.SqlCall;
import org.polypheny.db.languages.sql.SqlCallBinding;
import org.polypheny.db.languages.sql.SqlOperatorBinding;
import org.polypheny.db.languages.sql.SqlSelect;
import org.polypheny.db.languages.sql.SqlSpecialOperator;
import org.polypheny.db.languages.sql.SqlWriter;
import org.polypheny.db.languages.sql.validate.SqlValidator;
import org.polypheny.db.languages.sql.validate.SqlValidatorNamespace;
import org.polypheny.db.languages.sql.validate.SqlValidatorScope;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
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
    public RelDataType inferReturnType( SqlOperatorBinding opBinding ) {
        RelDataType type =
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


    private RelDataType getComponentType( RelDataTypeFactory typeFactory, List<RelDataType> argTypes ) {
        return typeFactory.leastRestrictive( argTypes );
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        final List<RelDataType> argTypes =
                PolyTypeUtil.deriveAndCollectTypes(
                        callBinding.getValidator(),
                        callBinding.getScope(),
                        callBinding.operands() );
        final RelDataType componentType =
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
    public RelDataType deriveType( SqlValidator validator, SqlValidatorScope scope, SqlCall call ) {
        SqlSelect subSelect = call.operand( 0 );
        subSelect.validateExpr( validator, scope );
        SqlValidatorNamespace ns = validator.getNamespace( subSelect );
        assert null != ns.getRowType();
        return PolyTypeUtil.createMultisetType(
                validator.getTypeFactory(),
                ns.getRowType(),
                false );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        writer.keyword( getName() );
        final SqlWriter.Frame frame = writer.startList( "(", ")" );
        assert call.operandCount() == 1;
        call.operand( 0 ).unparse( writer, leftPrec, rightPrec );
        writer.endList( frame );
    }


    @Override
    public boolean argumentMustBeScalar( int ordinal ) {
        return false;
    }
}


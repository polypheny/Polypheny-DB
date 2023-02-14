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


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.sql.language.SqlCallBinding;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Definition of the MAP constructor, <code>MAP [&lt;key&gt;, &lt;value&gt;, ...]</code>.
 *
 * This is an extension to standard SQL.
 */
public class SqlMapValueConstructor extends SqlMultisetValueConstructor {

    public SqlMapValueConstructor() {
        super( "MAP", Kind.MAP_VALUE_CONSTRUCTOR );
    }


    @Override
    public AlgDataType inferReturnType( OperatorBinding opBinding ) {
        Pair<AlgDataType, AlgDataType> type = getComponentTypes( opBinding.getTypeFactory(), opBinding.collectOperandTypes() );
        if ( null == type ) {
            return null;
        }
        return PolyTypeUtil.createMapType(
                opBinding.getTypeFactory(),
                type.left,
                type.right,
                false );
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        final List<AlgDataType> argTypes =
                PolyTypeUtil.deriveAndCollectTypes(
                        callBinding.getValidator(),
                        callBinding.getScope(),
                        callBinding.operands() );
        if ( argTypes.size() == 0 ) {
            throw callBinding.newValidationError( RESOURCE.mapRequiresTwoOrMoreArgs() );
        }
        if ( argTypes.size() % 2 > 0 ) {
            throw callBinding.newValidationError( RESOURCE.mapRequiresEvenArgCount() );
        }
        final Pair<AlgDataType, AlgDataType> componentType = getComponentTypes( callBinding.getTypeFactory(), argTypes );
        if ( null == componentType.left || null == componentType.right ) {
            if ( throwOnFailure ) {
                throw callBinding.newValidationError( RESOURCE.needSameTypeParameter() );
            }
            return false;
        }
        return true;
    }


    private Pair<AlgDataType, AlgDataType> getComponentTypes( AlgDataTypeFactory typeFactory, List<AlgDataType> argTypes ) {
        return Pair.of(
                typeFactory.leastRestrictive( Util.quotientList( argTypes, 2, 0 ) ),
                typeFactory.leastRestrictive( Util.quotientList( argTypes, 2, 1 ) ) );
    }

}


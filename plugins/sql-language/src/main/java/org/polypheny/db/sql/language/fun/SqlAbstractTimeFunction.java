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


import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlSyntax;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.util.Static;


/**
 * Base class for time functions such as "LOCALTIME", "LOCALTIME(n)".
 */
public class SqlAbstractTimeFunction extends SqlFunction {

    private static final PolyOperandTypeChecker OTC_CUSTOM = OperandTypes.or( OperandTypes.POSITIVE_INTEGER_LITERAL, OperandTypes.NILADIC );

    private final PolyType typeName;


    public SqlAbstractTimeFunction( String name, PolyType typeName ) {
        super( name, Kind.OTHER_FUNCTION, null, null, OTC_CUSTOM, FunctionCategory.TIMEDATE );
        this.typeName = typeName;
    }


    @Override
    public SqlSyntax getSqlSyntax() {
        return SqlSyntax.FUNCTION_ID;
    }


    @Override
    public AlgDataType inferReturnType( OperatorBinding opBinding ) {
        // REVIEW jvs: Need to take care of time zones.
        int precision = 0;
        if ( opBinding.getOperandCount() == 1 ) {
            AlgDataType type = opBinding.getOperandType( 0 );
            if ( PolyTypeUtil.isNumeric( type ) ) {
                precision = opBinding.getOperandLiteralValue( 0, PolyType.INTEGER ).asNumber().intValue();
            }
        }
        assert precision >= 0;
        if ( precision > PolyType.MAX_DATETIME_PRECISION ) {
            throw opBinding.newError( Static.RESOURCE.argumentMustBeValidPrecision( opBinding.getOperator().getName(), 0, PolyType.MAX_DATETIME_PRECISION ) );
        }
        return opBinding.getTypeFactory().createPolyType( typeName, precision );
    }


    // All of the time functions are increasing. Not strictly increasing.
    @Override
    public Monotonicity getMonotonicity( OperatorBinding call ) {
        return Monotonicity.INCREASING;
    }


    // Plans referencing context variables should never be cached
    @Override
    public boolean isDynamicFunction() {
        return true;
    }

}


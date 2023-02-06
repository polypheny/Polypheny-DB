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


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.util.ReflectiveSqlOperatorTable;
import org.polypheny.db.type.PolyTypeTransforms;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * Operator table that contains only Oracle-specific functions and operators.
 */
public class OracleSqlOperatorTable extends ReflectiveSqlOperatorTable {

    /**
     * The table of contains Oracle-specific operators.
     */
    private static OracleSqlOperatorTable instance;

    /**
     * Return type inference for {@code DECODE}.
     */
    protected static final PolyReturnTypeInference DECODE_RETURN_TYPE =
            opBinding -> {
                final List<AlgDataType> list = new ArrayList<>();
                for ( int i = 1, n = opBinding.getOperandCount(); i < n; i++ ) {
                    if ( i < n - 1 ) {
                        ++i;
                    }
                    list.add( opBinding.getOperandType( i ) );
                }
                final AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
                AlgDataType type = typeFactory.leastRestrictive( list );
                if ( opBinding.getOperandCount() % 2 == 1 ) {
                    type = typeFactory.createTypeWithNullability( type, true );
                }
                return type;
            };

    /**
     * The "DECODE(v, v1, result1, [v2, result2, ...], resultN)" function.
     */
    public static final SqlFunction DECODE =
            new SqlFunction(
                    "DECODE",
                    Kind.DECODE,
                    DECODE_RETURN_TYPE,
                    null,
                    OperandTypes.VARIADIC,
                    FunctionCategory.SYSTEM );

    /**
     * The "NVL(value, value)" function.
     */
    public static final SqlFunction NVL =
            new SqlFunction(
                    "NVL",
                    Kind.NVL,
                    ReturnTypes.cascade( ReturnTypes.LEAST_RESTRICTIVE, PolyTypeTransforms.TO_NULLABLE_ALL ),
                    null,
                    OperandTypes.SAME_SAME,
                    FunctionCategory.SYSTEM );

    /**
     * The "LTRIM(string)" function.
     */
    public static final SqlFunction LTRIM =
            new SqlFunction(
                    "LTRIM",
                    Kind.LTRIM,
                    ReturnTypes.cascade( ReturnTypes.ARG0, PolyTypeTransforms.TO_NULLABLE, PolyTypeTransforms.TO_VARYING ),
                    null,
                    OperandTypes.STRING,
                    FunctionCategory.STRING );

    /**
     * The "RTRIM(string)" function.
     */
    public static final SqlFunction RTRIM =
            new SqlFunction(
                    "RTRIM",
                    Kind.RTRIM,
                    ReturnTypes.cascade( ReturnTypes.ARG0, PolyTypeTransforms.TO_NULLABLE, PolyTypeTransforms.TO_VARYING ),
                    null,
                    OperandTypes.STRING,
                    FunctionCategory.STRING );

    /**
     * Oracle's "SUBSTR(string, position [, substringLength ])" function.
     *
     * It has similar semantics to standard SQL's {@code OperatorRegistry.get( OperatorName.SUBSTRING )} function but different syntax.
     */
    public static final SqlFunction SUBSTR =
            new SqlFunction(
                    "SUBSTR",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.ARG0_NULLABLE_VARYING,
                    null,
                    null,
                    FunctionCategory.STRING );

    /**
     * The "GREATEST(value, value)" function.
     */
    public static final SqlFunction GREATEST =
            new SqlFunction(
                    "GREATEST",
                    Kind.GREATEST,
                    ReturnTypes.cascade( ReturnTypes.LEAST_RESTRICTIVE, PolyTypeTransforms.TO_NULLABLE ),
                    null,
                    OperandTypes.SAME_VARIADIC,
                    FunctionCategory.SYSTEM );

    /**
     * The "LEAST(value, value)" function.
     */
    public static final SqlFunction LEAST =
            new SqlFunction(
                    "LEAST",
                    Kind.LEAST,
                    ReturnTypes.cascade( ReturnTypes.LEAST_RESTRICTIVE, PolyTypeTransforms.TO_NULLABLE ),
                    null,
                    OperandTypes.SAME_VARIADIC,
                    FunctionCategory.SYSTEM );

    /**
     * The <code>TRANSLATE(<i>string_expr</i>, <i>search_chars</i>, <i>replacement_chars</i>)</code> function returns <i>string_expr</i> with all occurrences of each character in
     * <i>search_chars</i> replaced by its corresponding character in <i>replacement_chars</i>.
     *
     * It is not defined in the SQL standard, but occurs in Oracle and PostgreSQL.
     */
    public static final SqlFunction TRANSLATE3 = new SqlTranslate3Function();


    /**
     * Returns the Oracle operator table, creating it if necessary.
     */
    public static synchronized OracleSqlOperatorTable instance() {
        if ( instance == null ) {
            // Creates and initializes the standard operator table.
            // Uses two-phase construction, because we can't initialize the table until the constructor of the sub-class has completed.
            instance = new OracleSqlOperatorTable();
            instance.init();
        }
        return instance;
    }

}


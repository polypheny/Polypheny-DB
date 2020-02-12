/*
 * Copyright 2019-2020 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.sql.fun;


import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.SqlAggFunction;
import org.polypheny.db.sql.SqlFunctionCategory;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.type.OperandTypes;
import org.polypheny.db.sql.type.ReturnTypes;
import org.polypheny.db.sql.type.SameOperandTypeChecker;
import org.polypheny.db.sql.type.SqlReturnTypeInference;
import org.polypheny.db.sql.type.SqlSingleOperandTypeChecker;
import org.polypheny.db.sql.type.SqlTypeFamily;
import org.polypheny.db.sql.type.SqlTypeTransform;
import org.polypheny.db.sql.type.SqlTypeTransforms;
import org.polypheny.db.util.Optionality;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;


/**
 * <code>LEAD</code> and <code>LAG</code> aggregate functions return the value of given expression evaluated at given offset.
 */
public class SqlLeadLagAggFunction extends SqlAggFunction {

    private static final SqlSingleOperandTypeChecker OPERAND_TYPES =
            OperandTypes.or(
                    OperandTypes.ANY,
                    OperandTypes.family( SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC ),
                    OperandTypes.and(
                            OperandTypes.family( SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC, SqlTypeFamily.ANY ),
                            // Arguments 1 and 3 must have same type
                            new SameOperandTypeChecker( 3 ) {
                                @Override
                                protected List<Integer>
                                getOperandList( int operandCount ) {
                                    return ImmutableList.of( 0, 2 );
                                }
                            } ) );

    private static final SqlReturnTypeInference RETURN_TYPE =
            ReturnTypes.cascade( ReturnTypes.ARG0, ( binding, type ) -> {
                // Result is NOT NULL if NOT NULL default value is provided
                SqlTypeTransform transform;
                if ( binding.getOperandCount() < 3 ) {
                    transform = SqlTypeTransforms.FORCE_NULLABLE;
                } else {
                    RelDataType defValueType = binding.getOperandType( 2 );
                    transform = defValueType.isNullable()
                            ? SqlTypeTransforms.FORCE_NULLABLE
                            : SqlTypeTransforms.TO_NOT_NULLABLE;
                }
                return transform.transformType( binding, type );
            } );


    public SqlLeadLagAggFunction( SqlKind kind ) {
        super(
                kind.name(),
                null,
                kind,
                RETURN_TYPE,
                null,
                OPERAND_TYPES,
                SqlFunctionCategory.NUMERIC,
                false,
                true,
                Optionality.FORBIDDEN );
        Preconditions.checkArgument( kind == SqlKind.LEAD || kind == SqlKind.LAG );
    }


    @Override
    public boolean allowsFraming() {
        return false;
    }

}


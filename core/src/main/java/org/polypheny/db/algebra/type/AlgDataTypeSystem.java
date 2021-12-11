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

package org.polypheny.db.algebra.type;


import org.polypheny.db.type.PolyType;


/**
 * Type system.
 *
 * Provides behaviors concerning type limits and behaviors. For example, in the default system, a DECIMAL can have maximum precision 19, but Hive overrides to 38.
 *
 * The default implementation is {@link #DEFAULT}.
 */
public interface AlgDataTypeSystem {

    /**
     * Default type system.
     */
    AlgDataTypeSystem DEFAULT = new AlgDataTypeSystemImpl() {
    };

    /**
     * Returns the maximum scale of a given type.
     */
    int getMaxScale( PolyType typeName );

    /**
     * Returns default precision for this type if supported, otherwise -1 if precision is either unsupported or must be specified explicitly.
     *
     * @return Default precision
     */
    int getDefaultPrecision( PolyType typeName );

    /**
     * Returns the maximum precision (or length) allowed for this type, or -1 if precision/length are not applicable for this type.
     *
     * @return Maximum allowed precision
     */
    int getMaxPrecision( PolyType typeName );

    /**
     * Returns the maximum scale of a NUMERIC or DECIMAL type.
     */
    int getMaxNumericScale();

    /**
     * Returns the maximum precision of a NUMERIC or DECIMAL type.
     */
    int getMaxNumericPrecision();

    /**
     * Returns the LITERAL string for the type, either PREFIX/SUFFIX.
     */
    String getLiteral( PolyType typeName, boolean isPrefix );

    /**
     * Returns whether the type is case sensitive.
     */
    boolean isCaseSensitive( PolyType typeName );

    /**
     * Returns whether the type can be auto increment.
     */
    boolean isAutoincrement( PolyType typeName );

    /**
     * Returns the numeric type radix, typically 2 or 10.
     * 0 means "not applicable".
     */
    int getNumTypeRadix( PolyType typeName );

    /**
     * Returns the return type of a call to the {@code SUM} aggregate function, inferred from its argument type.
     */
    AlgDataType deriveSumType( AlgDataTypeFactory typeFactory, AlgDataType argumentType );

    /**
     * Returns the return type of a call to the {@code AVG}, {@code STDDEV} or {@code VAR} aggregate functions, inferred from its argument type.
     */
    AlgDataType deriveAvgAggType( AlgDataTypeFactory typeFactory, AlgDataType argumentType );

    /**
     * Returns the return type of a call to the {@code COVAR} aggregate function, inferred from its argument types.
     */
    AlgDataType deriveCovarType( AlgDataTypeFactory typeFactory, AlgDataType arg0Type, AlgDataType arg1Type );

    /**
     * Returns the return type of the {@code CUME_DIST} and {@code PERCENT_RANK} aggregate functions.
     */
    AlgDataType deriveFractionalRankType( AlgDataTypeFactory typeFactory );

    /**
     * Returns the return type of the {@code NTILE}, {@code RANK}, {@code DENSE_RANK}, and {@code ROW_NUMBER} aggregate functions.
     */
    AlgDataType deriveRankType( AlgDataTypeFactory typeFactory );

    /**
     * Whether two record types are considered distinct if their field names are the same but in different cases.
     */
    boolean isSchemaCaseSensitive();

    /**
     * Whether the least restrictive type of a number of CHAR types of different lengths should be a VARCHAR type. And similarly BINARY to VARBINARY.
     */
    boolean shouldConvertRaggedUnionTypesToVarying();

}


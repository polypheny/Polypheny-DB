/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.sql.type;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorBinding;
import com.google.common.collect.ImmutableList;


/**
 * Strategy to infer the type of an operator call from the type of the operands by using one {@link SqlReturnTypeInference} rule and a combination of {@link SqlTypeTransform}s
 */
public class SqlTypeTransformCascade implements SqlReturnTypeInference {

    private final SqlReturnTypeInference rule;
    private final ImmutableList<SqlTypeTransform> transforms;


    /**
     * Creates a SqlTypeTransformCascade from a rule and an array of one or more transforms.
     */
    public SqlTypeTransformCascade( SqlReturnTypeInference rule, SqlTypeTransform... transforms ) {
        assert rule != null;
        assert transforms.length > 0;
        this.rule = rule;
        this.transforms = ImmutableList.copyOf( transforms );
    }


    @Override
    public RelDataType inferReturnType( SqlOperatorBinding opBinding ) {
        RelDataType ret = rule.inferReturnType( opBinding );
        if ( ret == null ) {
            // inferReturnType may return null; transformType does not accept or
            // return null types
            return null;
        }
        for ( SqlTypeTransform transform : transforms ) {
            ret = transform.transformType( opBinding, ret );
        }
        return ret;
    }
}


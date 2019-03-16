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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;


/**
 * Returns the first type that matches a set of given {@link SqlTypeName}s. If no match could be found, null is returned.
 */
public class MatchReturnTypeInference implements SqlReturnTypeInference {

    private final int start;
    private final List<SqlTypeName> typeNames;


    /**
     * Returns the first type of typeName at or after position start (zero based).
     *
     * @see #MatchReturnTypeInference(int, SqlTypeName[])
     */
    public MatchReturnTypeInference( int start, SqlTypeName... typeNames ) {
        this( start, ImmutableList.copyOf( typeNames ) );
    }


    /**
     * Returns the first type matching any type in typeNames at or after position start (zero based).
     */
    public MatchReturnTypeInference( int start, Iterable<SqlTypeName> typeNames ) {
        Preconditions.checkArgument( start >= 0 );
        this.start = start;
        this.typeNames = ImmutableList.copyOf( typeNames );
        Preconditions.checkArgument( !this.typeNames.isEmpty() );
    }


    public RelDataType inferReturnType( SqlOperatorBinding opBinding ) {
        for ( int i = start; i < opBinding.getOperandCount(); i++ ) {
            RelDataType argType = opBinding.getOperandType( i );
            if ( SqlTypeUtil.isOfSameTypeName( typeNames, argType ) ) {
                return argType;
            }
        }
        return null;
    }
}


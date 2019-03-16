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

package ch.unibas.dmi.dbis.polyphenydb.sql;


import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinRelType;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import java.util.Locale;
import org.apache.calcite.linq4j.CorrelateJoinType;


/**
 * Enumeration representing different join types used in correlation relations.
 */
public enum SemiJoinType {
    /**
     * Inner join
     */
    INNER,

    /**
     * Left-outer join
     */
    LEFT,

    /**
     * Semi-join
     * Similar to from A ... where a in (select b from B ...)
     */
    SEMI,

    /**
     * Anti-join
     * Similar to from A ... where a NOT in (select b from B ...)
     * Note: if B.b is nullable and B has nulls, no rows must be returned
     */
    ANTI;

    /**
     * Lower-case name.
     */
    public final String lowerName = name().toLowerCase( Locale.ROOT );


    /**
     * Creates a parse-tree node representing an occurrence of this condition type keyword at a particular position in the parsed text.
     */
    public SqlLiteral symbol( SqlParserPos pos ) {
        return SqlLiteral.createSymbol( this, pos );
    }


    public static SemiJoinType of( JoinRelType joinType ) {
        switch ( joinType ) {
            case INNER:
                return INNER;
            case LEFT:
                return LEFT;
        }
        throw new IllegalArgumentException( "Unsupported join type for semi-join " + joinType );
    }


    public JoinRelType toJoinType() {
        switch ( this ) {
            case INNER:
                return JoinRelType.INNER;
            case LEFT:
                return JoinRelType.LEFT;
        }
        throw new IllegalStateException( "Unable to convert " + this + " to JoinRelType" );
    }


    public CorrelateJoinType toLinq4j() {
        switch ( this ) {
            case INNER:
                return CorrelateJoinType.INNER;
            case LEFT:
                return CorrelateJoinType.LEFT;
            case SEMI:
                return CorrelateJoinType.SEMI;
            case ANTI:
                return CorrelateJoinType.ANTI;
        }
        throw new IllegalStateException( "Unable to convert " + this + " to JoinRelType" );
    }


    public boolean returnsJustFirstInput() {
        switch ( this ) {
            case INNER:
            case LEFT:
                return false;
            case SEMI:
            case ANTI:
                return true;
        }
        throw new IllegalStateException( "Unable to convert " + this + " to JoinRelType" );
    }
}


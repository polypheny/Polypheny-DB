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

package org.polypheny.db.algebra.constant;


import java.util.Locale;
import org.apache.calcite.linq4j.CorrelateJoinType;
import org.polypheny.db.algebra.core.JoinAlgType;


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


    public static SemiJoinType of( JoinAlgType joinType ) {
        switch ( joinType ) {
            case INNER:
                return INNER;
            case LEFT:
                return LEFT;
        }
        throw new IllegalArgumentException( "Unsupported join type for semi-join " + joinType );
    }


    public JoinAlgType toJoinType() {
        switch ( this ) {
            case INNER:
                return JoinAlgType.INNER;
            case LEFT:
                return JoinAlgType.LEFT;
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


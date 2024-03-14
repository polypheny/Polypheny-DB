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

package org.polypheny.db.algebra.constant;


import java.util.Locale;
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
        return switch ( joinType ) {
            case INNER -> INNER;
            case LEFT -> LEFT;
            default -> throw new IllegalArgumentException( "Unsupported join type for semi-join " + joinType );
        };
    }


    public JoinAlgType toJoinType() {
        return switch ( this ) {
            case INNER -> JoinAlgType.INNER;
            case LEFT -> JoinAlgType.LEFT;
            default -> throw new IllegalStateException( "Unable to convert " + this + " to JoinAlgType" );
        };
    }


    public JoinType toLinq4j() {
        return switch ( this ) {
            case INNER -> JoinType.INNER;
            case LEFT -> JoinType.LEFT;
            case SEMI -> JoinType.LEFT_SEMI_JOIN;
            case ANTI -> JoinType.COMMA;
        };
    }


    public boolean returnsJustFirstInput() {
        return switch ( this ) {
            case INNER, LEFT -> false;
            case SEMI, ANTI -> true;
        };
    }
}


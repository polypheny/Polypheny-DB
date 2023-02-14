/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adaptimizer.rndqueries;

import org.polypheny.db.adaptimizer.SeededClass;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

/**
 * Interface class for templates of randomly generated queries. Templates provide a way of
 *
 */
public interface QueryTemplate extends SeededClass {

    // -------------------------------------------------
    //        Wrapper Functions for random class
    // -------------------------------------------------

    /**
     * Returns the next "all" value for set operations.
     */
    boolean nextAll();

    /**
     * Returns the next Integer Value. This is a wrapper for the random object.
     */
    int nextInt( int bound );

    /**
     * Returns the next Long Value. This is a wrapper for the random object.
     */
    long nextLong();

    /**
     * Returns the next random binary operator type.
     */
    String nextBinaryOperatorType();

    /**
     * Returns the next random unary operator type.
     */
    String nextUnaryOperatorType();

    /**
     * Returns the next random table in a pair with the schema-name.
     */
    Pair<String, AdaptiveTableRecord> nextTable();

    /**
     * Returns true if the next Operator should be Unary.
     */
    boolean nextOperatorIsUnary();

    /**
     * Returns a pair of the provided column and its PolyType.
     */
    Pair<String, PolyType> nextColumn( String columnName );

    /**
     * Returns the next Random Column together with its PolyType.
     */
    Pair<String, PolyType> nextColumn( AdaptiveTableRecord record );

    /**
     * Returns the next random Join Type.
     */
    JoinAlgType nextJoinType();

    /**
     * Returns the next random Join Operation
     */
    String nextJoinOperation();

    /**
     * Returns the next random Filter Operation.
     */
    String nextFilterOperation();

    /**
     * Returns a unique String in the context of a tree generation iteration.
     * Used for providing unique aliases in set operations.
     * @param iter      Iteration of generation.
     */
    String nextUniqueString( int iter );

    // --------------------------------------------------------------------------------------------------
    //          Functions for Searching PolyType-Partners and Foreign Key References
    // --------------------------------------------------------------------------------------------------

    /**
     * Returns a pair of column-names that a join operation is possible on.
     */
    Pair<String, String> nextJoinColumns( AdaptiveTableRecord left, AdaptiveTableRecord right );

    // --------------------------------------------------------------------------------------------------
    //          Functions manipulating records for Set and Join Operations
    // --------------------------------------------------------------------------------------------------

    /**
     * Matches the columns of two records by reordering or duplication.
     */
    void matchOnColumnTypeSubsets( AdaptiveTableRecord left, AdaptiveTableRecord right );

    /**
     * Extends underlying records for a set operation.
     */
    void extendForSetOperation( AdaptiveTableRecord left, AdaptiveTableRecord right );

    /**
     * Extends underlying records for a join operation.
     */
    void extendForJoinOperation( AdaptiveTableRecord left, AdaptiveTableRecord right );


    // ------------------------------------------
    //         PolyType check functions
    // ------------------------------------------

    /**
     * Returns the PolyType which a column name corresponds with.
     */
    PolyType typeOf( String columnName );

    /**
     * Returns true if both given records have the same PolyTypes in the same order.
     */
    boolean haveSamePolyTypes( AdaptiveTableRecord left, AdaptiveTableRecord right );

    // ------------------------------------------
    //             Auxiliary Booleans
    // ------------------------------------------

    /**
     * Returns true if the generator should switch to join if set operations are not possible. (Will not try to make set ops work)
     */
    boolean switchToJoinIfSetOpNotViable();

    /**
     * Returns true if the generator should switch to set ops if join operations are not possible. (Will not try to make join work)
     */
    boolean switchToSetOpIfJoinNotPossible();

    /**
     * Activates the requirement for sort columns to be included in projections.
     */
    boolean projectSortColumnWorkaround();

    /**
     * Returns true if the given integer is the max depth in the template.
     **/
    boolean isMaxDepth( int depth );


}

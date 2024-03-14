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

package org.polypheny.db.algebra.enumerable;


import java.util.Objects;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.polypheny.db.algebra.AlgNode;


/**
 * An algebraic expression of one of the {@link EnumerableConvention} calling conventions.
 */
public interface EnumerableAlg extends AlgNode {


    /**
     * Creates a plan for this expression according to a calling convention.
     *
     * @param implementor Implementor
     * @param pref Preferred representation for rows in result expression
     * @return Plan for this expression according to a calling convention
     */
    Result implement( EnumerableAlgImplementor implementor, Prefer pref );

    /**
     * Preferred physical type.
     */
    enum Prefer {
        /**
         * Records must be represented as arrays.
         */
        ARRAY,
        /**
         * Consumer would prefer that records are represented as arrays, but can accommodate records represented as objects.
         */
        ARRAY_NICE,
        /**
         * Records must be represented as objects.
         */
        CUSTOM,
        /**
         * Consumer would prefer that records are represented as objects, but can accommodate records represented as arrays.
         */
        CUSTOM_NICE,
        /**
         * Consumer has no preferred representation.
         */
        ANY;


        public JavaTupleFormat preferCustom() {
            return prefer( JavaTupleFormat.CUSTOM );
        }


        public JavaTupleFormat preferArray() {
            return prefer( JavaTupleFormat.ARRAY );
        }


        public JavaTupleFormat prefer( JavaTupleFormat format ) {
            return switch ( this ) {
                case CUSTOM -> JavaTupleFormat.CUSTOM;
                case ARRAY -> JavaTupleFormat.ARRAY;
                default -> format;
            };
        }


        public Prefer of( JavaTupleFormat format ) {
            if ( Objects.requireNonNull( format ) == JavaTupleFormat.ARRAY ) {
                return ARRAY;
            }
            return CUSTOM;
        }
    }


    /**
     * Result of implementing an enumerable relational expression by generating Java code.
     *
     * @param physType Describes the Java type returned by this relational expression, and the mapping between it and the fields of the logical row type.
     */
    record Result(BlockStatement block, PhysType physType, JavaTupleFormat format) {

    }

}


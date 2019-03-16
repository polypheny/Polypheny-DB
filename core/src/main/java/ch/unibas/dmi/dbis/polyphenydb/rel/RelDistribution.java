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

package ch.unibas.dmi.dbis.polyphenydb.rel;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelMultipleTrait;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.Mappings.TargetMapping;
import java.util.List;
import javax.annotation.Nonnull;


/**
 * Description of the physical distribution of a relational expression.
 *
 * TBD:
 * <ul>
 * <li>Can we shorten {@link Type#HASH_DISTRIBUTED} to HASH, etc.</li>
 * <li>Do we need {@link RelDistributions}.DEFAULT?</li>
 * <li>{@link RelDistributionTraitDef#convert} does not create specific physical operators as it does in Drill. Drill will need to create rules; or we could allow "converters" to be registered with the planner that are not trait-defs.</li>
 * </ul>
 */
public interface RelDistribution extends RelMultipleTrait {

    /**
     * Returns the type of distribution.
     */
    @Nonnull
    Type getType();

    /**
     * Returns the ordinals of the key columns.
     *
     * Order is important for some types (RANGE); other types (HASH) consider it unimportant but impose an arbitrary order; other types (BROADCAST, SINGLETON) never have keys.
     */
    @Nonnull
    List<Integer> getKeys();

    RelDistribution apply( TargetMapping mapping );

    /**
     * Type of distribution.
     */
    enum Type {
        /**
         * There is only one instance of the stream. It sees all records.
         */
        SINGLETON( "single" ),

        /**
         * There are multiple instances of the stream, and each instance contains records whose keys hash to a particular hash value. Instances are disjoint; a given record appears on exactly one stream.
         */
        HASH_DISTRIBUTED( "hash" ),

        /**
         * There are multiple instances of the stream, and each instance contains records whose keys fall into a particular range. Instances are disjoint; a given record appears on exactly one stream.
         */
        RANGE_DISTRIBUTED( "range" ),

        /**
         * There are multiple instances of the stream, and each instance contains randomly chosen records. Instances are disjoint; a given record appears on exactly one stream.
         */
        RANDOM_DISTRIBUTED( "random" ),

        /**
         * There are multiple instances of the stream, and records are assigned to instances in turn. Instances are disjoint; a given record appears on exactly one stream.
         */
        ROUND_ROBIN_DISTRIBUTED( "rr" ),

        /**
         * There are multiple instances of the stream, and all records appear in each instance.
         */
        BROADCAST_DISTRIBUTED( "broadcast" ),

        /**
         * Not a valid distribution, but indicates that a consumer will accept any distribution.
         */
        ANY( "any" );

        public final String shortName;


        Type( String shortName ) {
            this.shortName = shortName;
        }
    }
}


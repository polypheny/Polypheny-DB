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

package ch.unibas.dmi.dbis.polyphenydb.sql2rel;


import ch.unibas.dmi.dbis.polyphenydb.rel.RelHomogeneousShuttle;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.CorrelationId;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexCorrelVariable;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexShuttle;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexSubQuery;
import com.google.common.collect.ImmutableSet;


/**
 * Rewrites relations to ensure the same correlation is referenced by the same correlation variable.
 */
public class DeduplicateCorrelateVariables extends RelHomogeneousShuttle {

    private final RexShuttle dedupRex;


    /**
     * Creates a DeduplicateCorrelateVariables.
     */
    private DeduplicateCorrelateVariables( RexBuilder builder, CorrelationId canonicalId, ImmutableSet<CorrelationId> alternateIds ) {
        dedupRex = new DeduplicateCorrelateVariablesShuttle( builder, canonicalId, alternateIds, this );
    }


    /**
     * Rewrites a relational expression, replacing alternate correlation variables with a canonical correlation variable.
     */
    public static RelNode go( RexBuilder builder, CorrelationId canonicalId, Iterable<? extends CorrelationId> alternateIds, RelNode r ) {
        return r.accept( new DeduplicateCorrelateVariables( builder, canonicalId, ImmutableSet.copyOf( alternateIds ) ) );
    }


    @Override
    public RelNode visit( RelNode other ) {
        RelNode next = super.visit( other );
        return next.accept( dedupRex );
    }


    /**
     * Replaces alternative names of correlation variable to its canonical name.
     */
    private static class DeduplicateCorrelateVariablesShuttle extends RexShuttle {

        private final RexBuilder builder;
        private final CorrelationId canonicalId;
        private final ImmutableSet<CorrelationId> alternateIds;
        private final DeduplicateCorrelateVariables shuttle;


        private DeduplicateCorrelateVariablesShuttle( RexBuilder builder, CorrelationId canonicalId, ImmutableSet<CorrelationId> alternateIds, DeduplicateCorrelateVariables shuttle ) {
            this.builder = builder;
            this.canonicalId = canonicalId;
            this.alternateIds = alternateIds;
            this.shuttle = shuttle;
        }


        @Override
        public RexNode visitCorrelVariable( RexCorrelVariable variable ) {
            if ( !alternateIds.contains( variable.id ) ) {
                return variable;
            }

            return builder.makeCorrel( variable.getType(), canonicalId );
        }


        @Override
        public RexNode visitSubQuery( RexSubQuery subQuery ) {
            if ( shuttle != null ) {
                RelNode r = subQuery.rel.accept( shuttle ); // look inside sub-queries
                if ( r != subQuery.rel ) {
                    subQuery = subQuery.clone( r );
                }
            }
            return super.visitSubQuery( subQuery );
        }
    }
}


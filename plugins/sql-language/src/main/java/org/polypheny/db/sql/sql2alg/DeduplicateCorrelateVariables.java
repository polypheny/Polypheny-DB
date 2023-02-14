/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.sql.sql2alg;


import com.google.common.collect.ImmutableSet;
import org.polypheny.db.algebra.AlgHomogeneousShuttle;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCorrelVariable;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexSubQuery;


/**
 * Rewrites relations to ensure the same correlation is referenced by the same correlation variable.
 */
public class DeduplicateCorrelateVariables extends AlgHomogeneousShuttle {

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
    public static AlgNode go( RexBuilder builder, CorrelationId canonicalId, Iterable<? extends CorrelationId> alternateIds, AlgNode r ) {
        return r.accept( new DeduplicateCorrelateVariables( builder, canonicalId, ImmutableSet.copyOf( alternateIds ) ) );
    }


    @Override
    public AlgNode visit( AlgNode other ) {
        AlgNode next = super.visit( other );
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
                AlgNode r = subQuery.alg.accept( shuttle ); // look inside sub-queries
                if ( r != subQuery.alg ) {
                    subQuery = subQuery.clone( r );
                }
            }
            return super.visitSubQuery( subQuery );
        }

    }

}


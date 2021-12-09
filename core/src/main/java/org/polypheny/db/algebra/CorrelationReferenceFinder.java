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

package org.polypheny.db.algebra;


import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.rex.RexCorrelVariable;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexSubQuery;


/**
 * Shuttle that finds references to a given {@link CorrelationId} within a tree of {@link AlgNode}s.
 */
public abstract class CorrelationReferenceFinder extends AlgHomogeneousShuttle {

    private final MyRexVisitor rexVisitor;


    /**
     * Creates CorrelationReferenceFinder.
     */
    protected CorrelationReferenceFinder() {
        rexVisitor = new MyRexVisitor( this );
    }


    protected abstract RexNode handle( RexFieldAccess fieldAccess );


    @Override
    public AlgNode visit( AlgNode other ) {
        AlgNode next = super.visit( other );
        return next.accept( rexVisitor );
    }


    /**
     * Replaces alternative names of correlation variable to its canonical name.
     */
    private static class MyRexVisitor extends RexShuttle {

        private final CorrelationReferenceFinder finder;


        private MyRexVisitor( CorrelationReferenceFinder finder ) {
            this.finder = finder;
        }


        @Override
        public RexNode visitFieldAccess( RexFieldAccess fieldAccess ) {
            if ( fieldAccess.getReferenceExpr() instanceof RexCorrelVariable ) {
                return finder.handle( fieldAccess );
            }
            return super.visitFieldAccess( fieldAccess );
        }


        @Override
        public RexNode visitSubQuery( RexSubQuery subQuery ) {
            final AlgNode r = subQuery.alg.accept( finder ); // look inside sub-queries
            if ( r != subQuery.alg ) {
                subQuery = subQuery.clone( r );
            }
            return super.visitSubQuery( subQuery );
        }

    }

}


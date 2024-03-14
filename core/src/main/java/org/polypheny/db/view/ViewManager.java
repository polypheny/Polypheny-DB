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

package org.polypheny.db.view;

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgFieldCollation.Direction;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.BiAlg;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.logical.common.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelCorrelate;
import org.polypheny.db.algebra.logical.relational.LogicalRelExchange;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelIntersect;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelMatch;
import org.polypheny.db.algebra.logical.relational.LogicalRelMinus;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelSort;
import org.polypheny.db.algebra.logical.relational.LogicalRelTableFunctionScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.logical.relational.LogicalRelViewScan;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalMaterializedView;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;


public class ViewManager {

    public static LogicalRelSort orderMaterialized( AlgNode other ) {
        int positionPrimary = other.getTupleType().getFields().size() - 1;
        AlgFieldCollation algFieldCollation = new AlgFieldCollation( positionPrimary, Direction.ASCENDING );
        AlgCollations.of( algFieldCollation );

        return LogicalRelSort.create( other, AlgCollations.of( algFieldCollation ), null, null );
    }


    public static AlgNode expandViewNode( AlgNode other ) {
        RexBuilder rexBuilder = other.getCluster().getRexBuilder();
        final List<RexNode> exprs = new ArrayList<>();
        final AlgDataType rowType = other.getTupleType();
        final int fieldCount = rowType.getFieldCount();
        for ( int i = 0; i < fieldCount; i++ ) {
            exprs.add( rexBuilder.makeInputRef( other, i ) );
        }

        AlgNode algNode = ((LogicalRelViewScan) other).getAlgNode();

        if ( algNode instanceof Project && algNode.getTupleType().getFieldNames().equals( other.getTupleType().getFieldNames() ) ) {
            return algNode;
        } else if ( algNode instanceof LogicalRelSort && algNode.getTupleType().getFieldNames().equals( other.getTupleType().getFieldNames() ) ) {
            return algNode;
        } else if ( algNode instanceof LogicalRelAggregate && algNode.getTupleType().getFieldNames().equals( other.getTupleType().getFieldNames() ) ) {
            return algNode;
        } else {
            return LogicalRelProject.create( algNode, exprs, other.getTupleType().getFieldNames() );
        }
    }


    public static class ViewVisitor extends AlgShuttleImpl {

        int depth = 0;
        final boolean doesSubstituteOrderBy;


        public ViewVisitor( boolean doesSubstituteOrderBy ) {
            this.doesSubstituteOrderBy = doesSubstituteOrderBy;
        }


        @Override
        public AlgNode visit( LogicalRelAggregate aggregate ) {
            handleNodeType( aggregate );
            depth++;
            return aggregate;
        }


        @Override
        public AlgNode visit( LogicalRelMatch match ) {
            handleNodeType( match );
            depth++;
            return match;
        }


        @Override
        public AlgNode visit( LogicalRelScan scan ) {
            if ( depth == 0 ) {
                return checkNode( scan );
            }
            handleNodeType( scan );
            depth++;
            return scan;
        }


        @Override
        public AlgNode visit( LogicalRelTableFunctionScan scan ) {
            handleNodeType( scan );
            depth++;
            return scan;
        }


        @Override
        public AlgNode visit( LogicalRelValues values ) {
            handleNodeType( values );
            depth++;
            return values;
        }


        @Override
        public AlgNode visit( LogicalRelFilter filter ) {
            handleNodeType( filter );
            depth++;
            return filter;
        }


        @Override
        public AlgNode visit( LogicalRelProject project ) {
            handleNodeType( project );
            depth++;
            return project;
        }


        @Override
        public AlgNode visit( LogicalRelJoin join ) {
            handleNodeType( join );
            depth++;
            return join;
        }


        @Override
        public AlgNode visit( LogicalRelCorrelate correlate ) {
            handleNodeType( correlate );
            depth++;
            return correlate;
        }


        @Override
        public AlgNode visit( LogicalRelUnion union ) {
            handleNodeType( union );
            depth++;
            return union;
        }


        @Override
        public AlgNode visit( LogicalRelIntersect intersect ) {
            handleNodeType( intersect );
            depth++;
            return intersect;
        }


        @Override
        public AlgNode visit( LogicalRelMinus minus ) {
            handleNodeType( minus );
            depth++;
            return minus;
        }


        @Override
        public AlgNode visit( LogicalRelSort sort ) {
            handleNodeType( sort );
            depth++;
            return sort;
        }


        @Override
        public AlgNode visit( LogicalRelExchange exchange ) {
            handleNodeType( exchange );
            depth++;
            return exchange;
        }


        @Override
        public AlgNode visit( LogicalConditionalExecute lce ) {
            handleNodeType( lce );
            depth++;
            return lce;
        }


        @Override
        public AlgNode visit( AlgNode other ) {
            handleNodeType( other );
            depth++;
            return other;
        }


        @Override
        public AlgNode visit( LogicalRelModify modify ) {
            handleNodeType( modify );
            depth++;
            return modify;
        }


        private void handleNodeType( AlgNode other ) {
            if ( other instanceof SingleAlg ) {
                other.replaceInput( 0, checkNode( ((SingleAlg) other).getInput() ) );
            } else if ( other instanceof BiAlg ) {
                other.replaceInput( 0, checkNode( other.getInput( 0 ) ) );
                other.replaceInput( 1, checkNode( other.getInput( 1 ) ) );
            }
        }


        public AlgNode checkNode( AlgNode other ) {
            /*if ( other instanceof LogicalRelViewScan ) {
                return expandViewNode( other );
            } else */
            if ( doesSubstituteOrderBy && other instanceof LogicalRelScan ) {
                LogicalTable catalogTable = other.getEntity().unwrap( LogicalTable.class ).orElseThrow();
                if ( catalogTable.entityType == EntityType.MATERIALIZED_VIEW && ((LogicalMaterializedView) catalogTable).isOrdered() ) {
                    return orderMaterialized( other );
                }
            }
            handleNodeType( other );
            return other;
        }


        /**
         * This method sends the visitor of into the {@link AlgNode} to replace
         * <code>LogicalViewScan</code> with <code>LogicalScan</code>
         *
         * As there is the possibility for the root {@link AlgNode} to be already be a view
         * it has to start with the AlgRoot and replace the initial AlgNode
         *
         * @param logicalRoot the AlgRoot before the transformation
         * @return the AlgRoot after replacing all <code>LogicalViewScan</code>s
         */
        public AlgRoot startSubstitution( AlgRoot logicalRoot ) {
            if ( logicalRoot.alg instanceof LogicalRelViewScan ) {
                AlgNode node = checkNode( logicalRoot.alg );
                return AlgRoot.of( node, logicalRoot.kind );
            } else {
                logicalRoot.alg.accept( this );
                return logicalRoot;
            }
        }

    }

}

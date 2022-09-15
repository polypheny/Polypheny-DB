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
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.core.TableFunctionScan;
import org.polypheny.db.algebra.logical.common.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.relational.LogicalAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalCorrelate;
import org.polypheny.db.algebra.logical.relational.LogicalExchange;
import org.polypheny.db.algebra.logical.relational.LogicalFilter;
import org.polypheny.db.algebra.logical.relational.LogicalIntersect;
import org.polypheny.db.algebra.logical.relational.LogicalJoin;
import org.polypheny.db.algebra.logical.relational.LogicalMatch;
import org.polypheny.db.algebra.logical.relational.LogicalMinus;
import org.polypheny.db.algebra.logical.relational.LogicalModify;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelViewScan;
import org.polypheny.db.algebra.logical.relational.LogicalScan;
import org.polypheny.db.algebra.logical.relational.LogicalSort;
import org.polypheny.db.algebra.logical.relational.LogicalUnion;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.EntityType;
import org.polypheny.db.catalog.entity.CatalogMaterializedView;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.prepare.AlgOptTableImpl;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.LogicalTable;


public class ViewManager {

    private static LogicalSort orderMaterialized( AlgNode other ) {
        int positionPrimary = other.getRowType().getFieldList().size() - 1;
        AlgFieldCollation algFieldCollation = new AlgFieldCollation( positionPrimary, Direction.ASCENDING );
        AlgCollations.of( algFieldCollation );

        return LogicalSort.create( other, AlgCollations.of( algFieldCollation ), null, null );
    }


    public static AlgNode expandViewNode( AlgNode other ) {
        RexBuilder rexBuilder = other.getCluster().getRexBuilder();
        final List<RexNode> exprs = new ArrayList<>();
        final AlgDataType rowType = other.getRowType();
        final int fieldCount = rowType.getFieldCount();
        for ( int i = 0; i < fieldCount; i++ ) {
            exprs.add( rexBuilder.makeInputRef( other, i ) );
        }

        AlgNode algNode = ((LogicalRelViewScan) other).getAlgNode();

        if ( algNode instanceof Project && algNode.getRowType().getFieldNames().equals( other.getRowType().getFieldNames() ) ) {
            return algNode;
        } else if ( algNode instanceof LogicalSort && algNode.getRowType().getFieldNames().equals( other.getRowType().getFieldNames() ) ) {
            return algNode;
        } else if ( algNode instanceof LogicalAggregate && algNode.getRowType().getFieldNames().equals( other.getRowType().getFieldNames() ) ) {
            return algNode;
        } else {
            return LogicalProject.create( algNode, exprs, other.getRowType().getFieldNames() );
        }
    }


    public static class ViewVisitor extends AlgShuttleImpl {

        int depth = 0;
        final boolean doesSubstituteOrderBy;


        public ViewVisitor( boolean doesSubstituteOrderBy ) {
            this.doesSubstituteOrderBy = doesSubstituteOrderBy;
        }


        @Override
        public AlgNode visit( LogicalAggregate aggregate ) {
            handleNodeType( aggregate );
            depth++;
            return aggregate;
        }


        @Override
        public AlgNode visit( LogicalMatch match ) {
            handleNodeType( match );
            depth++;
            return match;
        }


        @Override
        public AlgNode visit( Scan scan ) {
            if ( depth == 0 ) {
                return checkNode( scan );
            }
            handleNodeType( scan );
            depth++;
            return scan;
        }


        @Override
        public AlgNode visit( TableFunctionScan scan ) {
            handleNodeType( scan );
            depth++;
            return scan;
        }


        @Override
        public AlgNode visit( LogicalValues values ) {
            handleNodeType( values );
            depth++;
            return values;
        }


        @Override
        public AlgNode visit( LogicalFilter filter ) {
            handleNodeType( filter );
            depth++;
            return filter;
        }


        @Override
        public AlgNode visit( LogicalProject project ) {
            handleNodeType( project );
            depth++;
            return project;
        }


        @Override
        public AlgNode visit( LogicalJoin join ) {
            handleNodeType( join );
            depth++;
            return join;
        }


        @Override
        public AlgNode visit( LogicalCorrelate correlate ) {
            handleNodeType( correlate );
            depth++;
            return correlate;
        }


        @Override
        public AlgNode visit( LogicalUnion union ) {
            handleNodeType( union );
            depth++;
            return union;
        }


        @Override
        public AlgNode visit( LogicalIntersect intersect ) {
            handleNodeType( intersect );
            depth++;
            return intersect;
        }


        @Override
        public AlgNode visit( LogicalMinus minus ) {
            handleNodeType( minus );
            depth++;
            return minus;
        }


        @Override
        public AlgNode visit( LogicalSort sort ) {
            handleNodeType( sort );
            depth++;
            return sort;
        }


        @Override
        public AlgNode visit( LogicalExchange exchange ) {
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
        public AlgNode visit( LogicalModify modify ) {
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
            if ( other instanceof LogicalRelViewScan ) {
                return expandViewNode( other );
            } else if ( doesSubstituteOrderBy && other instanceof LogicalScan ) {
                if ( other.getTable() instanceof AlgOptTableImpl ) {
                    if ( other.getTable().getTable() instanceof LogicalTable ) {
                        long tableId = ((LogicalTable) ((AlgOptTableImpl) other.getTable()).getTable()).getTableId();
                        CatalogTable catalogtable = Catalog.getInstance().getTable( tableId );
                        if ( catalogtable.entityType == EntityType.MATERIALIZED_VIEW && ((CatalogMaterializedView) catalogtable).isOrdered() ) {
                            return orderMaterialized( other );
                        }
                    }
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

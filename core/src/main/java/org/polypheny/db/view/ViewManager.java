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

package org.polypheny.db.view;

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogMaterialized;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.rel.BiRel;
import org.polypheny.db.rel.RelCollations;
import org.polypheny.db.rel.RelFieldCollation;
import org.polypheny.db.rel.RelFieldCollation.Direction;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelShuttleImpl;
import org.polypheny.db.rel.SingleRel;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.core.TableFunctionScan;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.rel.logical.LogicalAggregate;
import org.polypheny.db.rel.logical.LogicalConditionalExecute;
import org.polypheny.db.rel.logical.LogicalCorrelate;
import org.polypheny.db.rel.logical.LogicalExchange;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalIntersect;
import org.polypheny.db.rel.logical.LogicalJoin;
import org.polypheny.db.rel.logical.LogicalMatch;
import org.polypheny.db.rel.logical.LogicalMinus;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.logical.LogicalSort;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.logical.LogicalUnion;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.rel.logical.LogicalViewTableScan;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.LogicalTable;

public class ViewManager {

    private static LogicalSort orderMaterialized( RelNode other ) {
        int positionPrimary = other.getRowType().getFieldList().size() - 1;
        RelFieldCollation relFieldCollation = new RelFieldCollation( positionPrimary, Direction.ASCENDING );
        RelCollations.of( relFieldCollation );

        return LogicalSort.create( other, RelCollations.of( relFieldCollation ), null, null );
    }


    public static RelNode expandViewNode( RelNode other ) {

        RexBuilder rexBuilder = other.getCluster().getRexBuilder();
        final List<RexNode> exprs = new ArrayList<>();
        final RelDataType rowType = other.getRowType();
        final int fieldCount = rowType.getFieldCount();
        for ( int i = 0; i < fieldCount; i++ ) {
            exprs.add( rexBuilder.makeInputRef( other, i ) );
        }

        RelNode relNode = ((LogicalViewTableScan) other).getRelNode();

        if ( relNode instanceof Project && relNode.getRowType().getFieldNames().equals( other.getRowType().getFieldNames() ) ) {
            return relNode;
        } else if ( relNode instanceof LogicalSort && relNode.getRowType().getFieldNames().equals( other.getRowType().getFieldNames() ) ) {
            return relNode;
        } else if ( relNode instanceof LogicalAggregate && relNode.getRowType().getFieldNames().equals( other.getRowType().getFieldNames() ) ) {
            return relNode;
        } else {
            return LogicalProject.create( relNode, exprs, other.getRowType().getFieldNames() );
        }
    }


    public static class ViewVisitor extends RelShuttleImpl {

        int depth = 0;
        boolean doesSubstituteOrderBy;


        public ViewVisitor( boolean doesSubstituteOrderBy ) {
            this.doesSubstituteOrderBy = doesSubstituteOrderBy;
        }


        @Override
        public RelNode visit( LogicalAggregate aggregate ) {
            handleNodeType( aggregate );
            depth++;
            return aggregate;
        }


        @Override
        public RelNode visit( LogicalMatch match ) {
            handleNodeType( match );
            depth++;
            return match;
        }


        @Override
        public RelNode visit( TableScan scan ) {
            if ( depth == 0 ) {
                return checkNode( scan );
            }
            handleNodeType( scan );
            depth++;
            return scan;
        }


        @Override
        public RelNode visit( TableFunctionScan scan ) {
            handleNodeType( scan );
            depth++;
            return scan;
        }


        @Override
        public RelNode visit( LogicalValues values ) {
            handleNodeType( values );
            depth++;
            return values;
        }


        @Override
        public RelNode visit( LogicalFilter filter ) {
            handleNodeType( filter );
            depth++;
            return filter;
        }


        @Override
        public RelNode visit( LogicalProject project ) {
            handleNodeType( project );
            depth++;
            return project;
        }


        @Override
        public RelNode visit( LogicalJoin join ) {
            handleNodeType( join );
            depth++;
            return join;
        }


        @Override
        public RelNode visit( LogicalCorrelate correlate ) {
            handleNodeType( correlate );
            depth++;
            return correlate;
        }


        @Override
        public RelNode visit( LogicalUnion union ) {
            handleNodeType( union );
            depth++;
            return union;
        }


        @Override
        public RelNode visit( LogicalIntersect intersect ) {
            handleNodeType( intersect );
            depth++;
            return intersect;
        }


        @Override
        public RelNode visit( LogicalMinus minus ) {
            handleNodeType( minus );
            depth++;
            return minus;
        }


        @Override
        public RelNode visit( LogicalSort sort ) {
            handleNodeType( sort );
            depth++;
            return sort;
        }


        @Override
        public RelNode visit( LogicalExchange exchange ) {
            handleNodeType( exchange );
            depth++;
            return exchange;
        }


        @Override
        public RelNode visit( LogicalConditionalExecute lce ) {
            handleNodeType( lce );
            depth++;
            return lce;
        }


        @Override
        public RelNode visit( RelNode other ) {

            handleNodeType( other );
            depth++;
            return other;
        }


        private void handleNodeType( RelNode other ) {
            if ( other instanceof SingleRel ) {
                other.replaceInput( 0, checkNode( ((SingleRel) other).getInput() ) );
            } else if ( other instanceof BiRel ) {
                other.replaceInput( 0, checkNode( other.getInput( 0 ) ) );
                other.replaceInput( 1, checkNode( other.getInput( 1 ) ) );
            }
        }


        private RelNode checkNode( RelNode other ) {

            if ( other instanceof LogicalViewTableScan ) {
                return expandViewNode( other );
            } else if ( doesSubstituteOrderBy && other instanceof LogicalTableScan ) {

                if ( other.getTable() instanceof RelOptTableImpl ) {
                    if ( ((RelOptTableImpl) other.getTable()).getTable() instanceof LogicalTable ) {
                        long tableId = ((LogicalTable) ((RelOptTableImpl) other.getTable()).getTable()).getTableId();

                        CatalogTable catalogtable = Catalog.getInstance().getTable( tableId );

                        if ( catalogtable.isMaterialized() && ((CatalogMaterialized) catalogtable).isOrdered() ) {
                            return orderMaterialized( other );
                        }
                    }

                }
            }
            handleNodeType( other );
            return other;
        }

    }

}

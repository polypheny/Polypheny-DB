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

package org.polypheny.db.processing;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.PolyResult;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.BatchIterator;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.TableModify;
import org.polypheny.db.algebra.logical.LogicalBatchIterator;
import org.polypheny.db.algebra.logical.LogicalConstraintEnforcer;
import org.polypheny.db.algebra.logical.LogicalConstraintEnforcer.EnforcementInformation;
import org.polypheny.db.algebra.logical.LogicalJoin;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;

public class EnumerableAdjuster {


    public static AlgRoot adjustBatch( AlgRoot root, Statement statement ) {
        return AlgRoot.of( LogicalBatchIterator.create( root.alg, statement ), root.kind );
    }


    public static boolean needsAdjustment( AlgNode alg ) {
        if ( alg instanceof TableModify ) {
            return ((TableModify) alg).isUpdate();
        }

        boolean needsAdjustment = false;
        for ( AlgNode input : alg.getInputs() ) {
            needsAdjustment |= needsAdjustment( input );
        }
        return needsAdjustment;
    }


    public static AlgRoot attachOnQueryConstraints( AlgRoot root, Statement statement ) {
        return AlgRoot.of(
                LogicalConstraintEnforcer.create( root.alg, statement ),
                root.kind );
    }


    public static AlgRoot prepareJoins( AlgRoot root, Statement statement, QueryProcessor queryProcessor ) {
        JoinAdjuster adjuster = new JoinAdjuster( statement, queryProcessor );
        return AlgRoot.of( root.alg.accept( adjuster ), root.kind );
    }


    public static void attachOnCommitConstraints( AlgNode node, Statement statement ) {
        TableModify modify;
        // todo maybe use shuttle?
        if ( node instanceof TableModify ) {
            modify = (TableModify) node;
        } else if ( node instanceof BatchIterator ) {
            if ( node.getInput( 0 ) instanceof TableModify ) {
                modify = (TableModify) node.getInput( 0 );
            } else {
                throw new RuntimeException( "The tree did no conform, while generating the constraint enforcement query!" );
            }

        } else {
            throw new RuntimeException( "The tree did no conform, while generating the constraint enforcement query!" );
        }
        statement.getTransaction().getCatalogTables().add( LogicalConstraintEnforcer.getCatalogTable( modify ) );
    }


    public static List<EnforcementInformation> getConstraintAlg( Set<CatalogTable> catalogTables, Statement statement ) {
        return catalogTables
                .stream()
                .map( t -> LogicalConstraintEnforcer.getControl( t, statement ) )
                .filter( i -> i.getControl() != null )
                .collect( Collectors.toList() );
    }


    private static class JoinAdjuster extends AlgShuttleImpl {

        private final Statement statement;
        private final QueryProcessor queryProcessor;
        private final boolean fullPreExecute = false;


        public JoinAdjuster( Statement statement, QueryProcessor queryProcessor ) {
            this.statement = statement;
            this.queryProcessor = queryProcessor;
        }


        @Override
        // todo dl, rewrite extremely prototypey
        public AlgNode visit( LogicalJoin join ) {
            AlgBuilder builder = AlgBuilder.create( statement );
            RexBuilder rexBuilder = builder.getRexBuilder();
            AlgNode left = join.getLeft().accept( this );
            AlgNode right = join.getRight().accept( this );

            if ( join.getCondition() instanceof RexCall ) {
                return prepareOneSide( join, builder, rexBuilder, left, right );
            }
            join.replaceInput( 0, left );
            join.replaceInput( 1, right );
            return join;
            // extract underlying right operators which compare left to right
        }


        private AlgNode prepareOneSide( LogicalJoin join, AlgBuilder builder, RexBuilder rexBuilder, AlgNode left, AlgNode right ) {
            boolean preRouteRight = join.getJoinType() != JoinAlgType.LEFT
                    || (join.getJoinType() == JoinAlgType.INNER && left.getTable().getRowCount() < right.getTable().getRowCount());

            // potentially try to use the more restrictive side or do a cost model depending on a mix of size and restrictions
            ConditionExtractor extractor = new ConditionExtractor( preRouteRight, rexBuilder, left.getRowType().getFieldCount() );

            builder.push( preRouteRight ? right : left );

            join.accept( extractor );
            if ( extractor.filters.size() > 0 ) {
                builder.filter( extractor.getFilters() );
            }

            builder.project( extractor.getProjects() );

            if ( fullPreExecute ) {

                PolyResult result = queryProcessor.prepareQuery( AlgRoot.of( builder.build(), Kind.SELECT ), false );
                List<List<Object>> rows = result.getRows( statement, -1 );

                builder.push( preRouteRight ? left : right );

                List<RexNode> nodes = new ArrayList<>();

                for ( List<Object> row : rows ) {
                    List<RexNode> ands = new ArrayList<>();
                    int pos = 0;
                    for ( Object o : row ) {
                        RexInputRef ref = extractor.otherProjects.get( pos );
                        ands.add(
                                rexBuilder.makeCall(
                                        OperatorRegistry.get( OperatorName.EQUALS ),
                                        builder.field( ref.getIndex() ),
                                        rexBuilder.makeLiteral( o, ref.getType(), false ) ) );
                        pos++;
                    }
                    if ( ands.size() > 1 ) {
                        nodes.add( rexBuilder.makeCall( OperatorRegistry.get( OperatorName.AND ), ands ) );
                    } else {
                        nodes.add( ands.get( 0 ) );
                    }
                }
                AlgNode prepared;
                if ( nodes.size() > 1 ) {
                    prepared = builder.filter( rexBuilder.makeCall( OperatorRegistry.get( OperatorName.OR ), nodes ) ).build();
                } else if ( nodes.size() == 1 ) {
                    prepared = builder.filter( nodes.get( 0 ) ).build();
                } else {
                    // there seems to be nothing in the pre-routed side, maybe we use an empty values?
                    prepared = preRouteRight ? left : right;
                }
                builder.push( preRouteRight ? prepared : left );
                builder.push( preRouteRight ? right : prepared );
                builder.join( join.getJoinType(), join.getCondition() );
            } else {
                // cannot project unused values away as then AlgNode would possibly be invalid
                AlgNode prepared = builder.build();
                builder.push( preRouteRight ? left : prepared );
                builder.push( preRouteRight ? prepared : right );
                builder.join( join.getJoinType(), join.getCondition() );
            }
            return builder.build();
        }


        public static class ConditionExtractor extends RexShuttle {

            private final boolean preRouteRight;
            private final RexBuilder rexBuilder;
            private final long leftSize;

            @Getter
            private final List<RexNode> filters = new ArrayList<>();
            @Getter
            private final List<RexInputRef> projects = new ArrayList<>();
            @Getter
            private final List<RexInputRef> otherProjects = new ArrayList<>();


            public ConditionExtractor( boolean preRouteRight, RexBuilder rexBuilder, long leftSize ) {
                this.preRouteRight = preRouteRight;
                this.rexBuilder = rexBuilder;
                this.leftSize = leftSize;
            }


            @Override
            public RexNode visitInputRef( RexInputRef inputRef ) {
                RexInputRef project = null;
                RexInputRef otherProject = null;

                if ( inputRef.getIndex() >= leftSize ) {
                    // is from right
                    if ( preRouteRight ) {
                        project = rexBuilder.makeInputRef( inputRef.getType(), (int) (inputRef.getIndex() - leftSize) );
                    } else {
                        // add not routed ref into other projection collection to use it after
                        otherProject = rexBuilder.makeInputRef( inputRef.getType(), (int) (inputRef.getIndex() - leftSize) );
                    }
                } else {
                    // is from left
                    if ( !preRouteRight ) {
                        project = rexBuilder.makeInputRef( inputRef.getType(), inputRef.getIndex() );
                    } else {
                        otherProject = rexBuilder.makeInputRef( inputRef.getType(), inputRef.getIndex() );
                    }
                }

                if ( project != null ) {
                    projects.add( project );
                }
                if ( otherProject != null ) {
                    otherProjects.add( otherProject );
                }

                // we only need to filter for the join condition later on, so this is enough
                return project;
            }


            @Override
            public RexNode visitCall( RexCall call ) {
                List<RexNode> nodes = call.operands.stream().map( c -> c.accept( this ) ).filter( Objects::nonNull ).collect( Collectors.toList() );

                if ( nodes.size() == 1 ) {
                    filters.add( nodes.get( 0 ) );
                    //return nodes.get( 0 );
                } else if ( nodes.size() == 0 ) {
                    return null;
                }

                /*switch ( call.op.getOperatorName() ) {
                    case EQUALS:
                    case NOT_EQUALS:
                        filters.add( rexBuilder.makeCall( call.op, nodes ) );
                        break;
                    case AND:
                    case OR:
                }*/
                return call;
            }

        }

    }


    static public class ConstraintTracker implements ConfigListener {

        private final TransactionManager manager;


        public ConstraintTracker( TransactionManager manager ) {
            this.manager = manager;
        }


        @Override
        public void onConfigChange( Config c ) {
            if ( !testConstraintsValid() ) {
                c.setBoolean( !c.getBoolean() );
                throw new RuntimeException( "Could not change the constraints." );
            }
        }


        @Override
        public void restart( Config c ) {
            if ( !testConstraintsValid() ) {
                c.setBoolean( !c.getBoolean() );
                throw new RuntimeException( "After restart the constraints where not longer enforceable." );
            }
        }


        private boolean testConstraintsValid() {
            if ( RuntimeConfig.FOREIGN_KEY_ENFORCEMENT.getBoolean() || RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.getBoolean() ) {
                try {
                    List<CatalogTable> tables = Catalog
                            .getInstance()
                            .getTables( null, null, null )
                            .stream()
                            .filter( t -> t.tableType == TableType.TABLE )
                            .collect( Collectors.toList() );
                    Transaction transaction = this.manager.startTransaction( "pa", "APP", false, "ConstraintEnforcement" );
                    Statement statement = transaction.createStatement();
                    QueryProcessor processor = statement.getQueryProcessor();
                    List<EnforcementInformation> infos = EnumerableAdjuster
                            .getConstraintAlg( new TreeSet<>( tables ), statement );
                    List<PolyResult> results = infos
                            .stream()
                            .map( s -> processor.prepareQuery( AlgRoot.of( s.getControl(), Kind.SELECT ), false ) )
                            .collect( Collectors.toList() );
                    List<List<List<Object>>> rows = results.stream()
                            .map( r -> r.getRows( statement, -1 ) )
                            .filter( r -> r.size() != 0 )
                            .collect( Collectors.toList() );

                    if ( rows.size() != 0 ) {
                        Integer index = (Integer) rows.get( 0 ).get( 0 ).get( 1 );
                        throw new TransactionException( infos.get( 0 ).getErrorMessages().get( index ) + "\nThere are violated constraints, the transaction was rolled back!" );
                    }

                } catch ( UnknownDatabaseException | UnknownSchemaException | UnknownUserException | TransactionException | GenericCatalogException e ) {
                    return false;
                }
            }
            return true;
        }

    }

}

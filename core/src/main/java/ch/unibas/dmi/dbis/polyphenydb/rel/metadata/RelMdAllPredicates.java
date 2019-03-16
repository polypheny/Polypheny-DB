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

package ch.unibas.dmi.dbis.polyphenydb.rel.metadata;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPredicateList;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepRelVertex;
import ch.unibas.dmi.dbis.polyphenydb.plan.volcano.RelSubset;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Exchange;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Join;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinRelType;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Sort;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Union;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexInputRef;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexTableInputRef;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexTableInputRef.RelTableRef;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexUtil;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Utility to extract Predicates that are present in the (sub)plan starting at this node.
 *
 * This should be used to infer whether same filters are applied on a given plan by materialized view rewriting rules.
 *
 * The output predicates might contain references to columns produced by TableScan operators ({@link RexTableInputRef}). In turn, each TableScan operator is identified uniquely by its qualified name and an identifier.
 *
 * If the provider cannot infer the lineage for any of the expressions contain in any of the predicates, it will return null. Observe that this is different from the empty list of predicates, which means that there are not predicates in the (sub)plan.
 */
public class RelMdAllPredicates implements MetadataHandler<BuiltInMetadata.AllPredicates> {

    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource( BuiltInMethod.ALL_PREDICATES.method, new RelMdAllPredicates() );


    public MetadataDef<BuiltInMetadata.AllPredicates> getDef() {
        return BuiltInMetadata.AllPredicates.DEF;
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.AllPredicates#getAllPredicates()}, invoked using reflection.
     *
     * @see RelMetadataQuery#getAllPredicates(RelNode)
     */
    public RelOptPredicateList getAllPredicates( RelNode rel, RelMetadataQuery mq ) {
        return null;
    }


    public RelOptPredicateList getAllPredicates( HepRelVertex rel, RelMetadataQuery mq ) {
        return mq.getAllPredicates( rel.getCurrentRel() );
    }


    public RelOptPredicateList getAllPredicates( RelSubset rel, RelMetadataQuery mq ) {
        return mq.getAllPredicates( Util.first( rel.getBest(), rel.getOriginal() ) );
    }


    /**
     * Extract predicates for a table scan.
     */
    public RelOptPredicateList getAllPredicates( TableScan table, RelMetadataQuery mq ) {
        return RelOptPredicateList.EMPTY;
    }


    /**
     * Extract predicates for a project.
     */
    public RelOptPredicateList getAllPredicates( Project project, RelMetadataQuery mq ) {
        return mq.getAllPredicates( project.getInput() );
    }


    /**
     * Add the Filter condition to the list obtained from the input.
     */
    public RelOptPredicateList getAllPredicates( Filter filter, RelMetadataQuery mq ) {
        final RelNode input = filter.getInput();
        final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        final RexNode pred = filter.getCondition();

        final RelOptPredicateList predsBelow = mq.getAllPredicates( input );
        if ( predsBelow == null ) {
            // Safety check
            return null;
        }

        // Extract input fields referenced by Filter condition
        final Set<RelDataTypeField> inputExtraFields = new LinkedHashSet<>();
        final RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder( inputExtraFields );
        pred.accept( inputFinder );
        final ImmutableBitSet inputFieldsUsed = inputFinder.inputBitSet.build();

        // Infer column origin expressions for given references
        final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
        for ( int idx : inputFieldsUsed ) {
            final RexInputRef ref = RexInputRef.of( idx, filter.getRowType().getFieldList() );
            final Set<RexNode> originalExprs = mq.getExpressionLineage( filter, ref );
            if ( originalExprs == null ) {
                // Bail out
                return null;
            }
            mapping.put( ref, originalExprs );
        }

        // Replace with new expressions and return union of predicates
        final Set<RexNode> allExprs = RelMdExpressionLineage.createAllPossibleExpressions( rexBuilder, pred, mapping );
        if ( allExprs == null ) {
            return null;
        }
        return predsBelow.union( rexBuilder, RelOptPredicateList.of( rexBuilder, allExprs ) );
    }


    /**
     * Add the Join condition to the list obtained from the input.
     */
    public RelOptPredicateList getAllPredicates( Join join, RelMetadataQuery mq ) {
        if ( join.getJoinType() != JoinRelType.INNER ) {
            // We cannot map origin of this expression.
            return null;
        }

        final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        final RexNode pred = join.getCondition();

        final Multimap<List<String>, RelTableRef> qualifiedNamesToRefs = HashMultimap.create();
        RelOptPredicateList newPreds = RelOptPredicateList.EMPTY;
        for ( RelNode input : join.getInputs() ) {
            final RelOptPredicateList inputPreds = mq.getAllPredicates( input );
            if ( inputPreds == null ) {
                // Bail out
                return null;
            }
            // Gather table references
            final Set<RelTableRef> tableRefs = mq.getTableReferences( input );
            if ( input == join.getLeft() ) {
                // Left input references remain unchanged
                for ( RelTableRef leftRef : tableRefs ) {
                    qualifiedNamesToRefs.put( leftRef.getQualifiedName(), leftRef );
                }
                newPreds = newPreds.union( rexBuilder, inputPreds );
            } else {
                // Right input references might need to be updated if there are table name clashes with left input
                final Map<RelTableRef, RelTableRef> currentTablesMapping = new HashMap<>();
                for ( RelTableRef rightRef : tableRefs ) {
                    int shift = 0;
                    Collection<RelTableRef> lRefs = qualifiedNamesToRefs.get( rightRef.getQualifiedName() );
                    if ( lRefs != null ) {
                        shift = lRefs.size();
                    }
                    currentTablesMapping.put( rightRef, RelTableRef.of( rightRef.getTable(), shift + rightRef.getEntityNumber() ) );
                }
                final List<RexNode> updatedPreds = Lists.newArrayList( Iterables.transform( inputPreds.pulledUpPredicates, e -> RexUtil.swapTableReferences( rexBuilder, e, currentTablesMapping ) ) );
                newPreds = newPreds.union( rexBuilder, RelOptPredicateList.of( rexBuilder, updatedPreds ) );
            }
        }

        // Extract input fields referenced by Join condition
        final Set<RelDataTypeField> inputExtraFields = new LinkedHashSet<>();
        final RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder( inputExtraFields );
        pred.accept( inputFinder );
        final ImmutableBitSet inputFieldsUsed = inputFinder.inputBitSet.build();

        // Infer column origin expressions for given references
        final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
        for ( int idx : inputFieldsUsed ) {
            final RexInputRef inputRef = RexInputRef.of( idx, join.getRowType().getFieldList() );
            final Set<RexNode> originalExprs = mq.getExpressionLineage( join, inputRef );
            if ( originalExprs == null ) {
                // Bail out
                return null;
            }
            final RexInputRef ref = RexInputRef.of( idx, join.getRowType().getFieldList() );
            mapping.put( ref, originalExprs );
        }

        // Replace with new expressions and return union of predicates
        final Set<RexNode> allExprs = RelMdExpressionLineage.createAllPossibleExpressions( rexBuilder, pred, mapping );
        if ( allExprs == null ) {
            return null;
        }
        return newPreds.union( rexBuilder, RelOptPredicateList.of( rexBuilder, allExprs ) );
    }


    /**
     * Extract predicates for an Aggregate.
     */
    public RelOptPredicateList getAllPredicates( Aggregate agg, RelMetadataQuery mq ) {
        return mq.getAllPredicates( agg.getInput() );
    }


    /**
     * Extract predicates for a Union.
     */
    public RelOptPredicateList getAllPredicates( Union union, RelMetadataQuery mq ) {
        final RexBuilder rexBuilder = union.getCluster().getRexBuilder();

        final Multimap<List<String>, RelTableRef> qualifiedNamesToRefs = HashMultimap.create();
        RelOptPredicateList newPreds = RelOptPredicateList.EMPTY;
        for ( int i = 0; i < union.getInputs().size(); i++ ) {
            final RelNode input = union.getInput( i );
            final RelOptPredicateList inputPreds = mq.getAllPredicates( input );
            if ( inputPreds == null ) {
                // Bail out
                return null;
            }
            // Gather table references
            final Set<RelTableRef> tableRefs = mq.getTableReferences( input );
            if ( i == 0 ) {
                // Left input references remain unchanged
                for ( RelTableRef leftRef : tableRefs ) {
                    qualifiedNamesToRefs.put( leftRef.getQualifiedName(), leftRef );
                }
                newPreds = newPreds.union( rexBuilder, inputPreds );
            } else {
                // Right input references might need to be updated if there are table name
                // clashes with left input
                final Map<RelTableRef, RelTableRef> currentTablesMapping = new HashMap<>();
                for ( RelTableRef rightRef : tableRefs ) {
                    int shift = 0;
                    Collection<RelTableRef> lRefs = qualifiedNamesToRefs.get( rightRef.getQualifiedName() );
                    if ( lRefs != null ) {
                        shift = lRefs.size();
                    }
                    currentTablesMapping.put( rightRef, RelTableRef.of( rightRef.getTable(), shift + rightRef.getEntityNumber() ) );
                }
                // Add to existing qualified names
                for ( RelTableRef newRef : currentTablesMapping.values() ) {
                    qualifiedNamesToRefs.put( newRef.getQualifiedName(), newRef );
                }
                // Update preds
                final List<RexNode> updatedPreds = Lists.newArrayList( Iterables.transform( inputPreds.pulledUpPredicates, e -> RexUtil.swapTableReferences( rexBuilder, e, currentTablesMapping ) ) );
                newPreds = newPreds.union( rexBuilder, RelOptPredicateList.of( rexBuilder, updatedPreds ) );
            }
        }
        return newPreds;
    }


    /**
     * Extract predicates for a Sort.
     */
    public RelOptPredicateList getAllPredicates( Sort sort, RelMetadataQuery mq ) {
        return mq.getAllPredicates( sort.getInput() );
    }


    /**
     * Extract predicates for an Exchange.
     */
    public RelOptPredicateList getAllPredicates( Exchange exchange, RelMetadataQuery mq ) {
        return mq.getAllPredicates( exchange.getInput() );
    }

}


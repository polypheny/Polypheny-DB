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

package org.polypheny.db.algebra.metadata;


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
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.Exchange;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptPredicateList;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.hep.HepAlgVertex;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexTableIndexRef;
import org.polypheny.db.rex.RexTableIndexRef.AlgTableRef;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Util;


/**
 * Utility to extract Predicates that are present in the (sub)plan starting at this node.
 * <p>
 * This should be used to infer whether same filters are applied on a given plan by materialized view rewriting rules.
 * <p>
 * The output predicates might contain references to columns produced by Scan operators ({@link RexTableIndexRef}). In turn, each Scan operator is identified uniquely by its qualified name and an identifier.
 * <p>
 * If the provider cannot infer the lineage for any of the expressions contain in any of the predicates, it will return null. Observe that this is different from the empty list of predicates, which means that there are not predicates in the (sub)plan.
 */
public class AlgMdAllPredicates implements MetadataHandler<BuiltInMetadata.AllPredicates> {

    public static final AlgMetadataProvider SOURCE = ReflectiveAlgMetadataProvider.reflectiveSource( new AlgMdAllPredicates(), BuiltInMethod.ALL_PREDICATES.method );


    @Override
    public MetadataDef<BuiltInMetadata.AllPredicates> getDef() {
        return BuiltInMetadata.AllPredicates.DEF;
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.AllPredicates#getAllPredicates()}, invoked using reflection.
     *
     * @see AlgMetadataQuery#getAllPredicates(AlgNode)
     */
    public AlgOptPredicateList getAllPredicates( AlgNode alg, AlgMetadataQuery mq ) {
        return null;
    }


    public AlgOptPredicateList getAllPredicates( HepAlgVertex alg, AlgMetadataQuery mq ) {
        return mq.getAllPredicates( alg.getCurrentAlg() );
    }


    public AlgOptPredicateList getAllPredicates( AlgSubset alg, AlgMetadataQuery mq ) {
        return mq.getAllPredicates( Util.first( alg.getBest(), alg.getOriginal() ) );
    }


    /**
     * Extract predicates for a table relScan.
     */
    public AlgOptPredicateList getAllPredicates( RelScan table, AlgMetadataQuery mq ) {
        return AlgOptPredicateList.EMPTY;
    }


    /**
     * Extract predicates for a project.
     */
    public AlgOptPredicateList getAllPredicates( Project project, AlgMetadataQuery mq ) {
        return mq.getAllPredicates( project.getInput() );
    }


    /**
     * Add the Filter condition to the list obtained from the input.
     */
    public AlgOptPredicateList getAllPredicates( Filter filter, AlgMetadataQuery mq ) {
        final AlgNode input = filter.getInput();
        final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        final RexNode pred = filter.getCondition();

        final AlgOptPredicateList predsBelow = mq.getAllPredicates( input );
        if ( predsBelow == null ) {
            // Safety check
            return null;
        }

        // Extract input fields referenced by Filter condition
        final Set<AlgDataTypeField> inputExtraFields = new LinkedHashSet<>();
        final AlgOptUtil.InputFinder inputFinder = new AlgOptUtil.InputFinder( inputExtraFields );
        pred.accept( inputFinder );
        final ImmutableBitSet inputFieldsUsed = inputFinder.inputBitSet.build();

        // Infer column origin expressions for given references
        final Map<RexIndexRef, Set<RexNode>> mapping = new LinkedHashMap<>();
        for ( int idx : inputFieldsUsed ) {
            final RexIndexRef ref = RexIndexRef.of( idx, filter.getTupleType().getFields() );
            final Set<RexNode> originalExprs = mq.getExpressionLineage( filter, ref );
            if ( originalExprs == null ) {
                // Bail out
                return null;
            }
            mapping.put( ref, originalExprs );
        }

        // Replace with new expressions and return union of predicates
        final Set<RexNode> allExprs = AlgMdExpressionLineage.createAllPossibleExpressions( rexBuilder, pred, mapping );
        if ( allExprs == null ) {
            return null;
        }
        return predsBelow.union( rexBuilder, AlgOptPredicateList.of( rexBuilder, allExprs ) );
    }


    /**
     * Add the Join condition to the list obtained from the input.
     */
    public AlgOptPredicateList getAllPredicates( Join join, AlgMetadataQuery mq ) {
        if ( join.getJoinType() != JoinAlgType.INNER ) {
            // We cannot map origin of this expression.
            return null;
        }

        final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        final RexNode pred = join.getCondition();

        final Multimap<List<String>, AlgTableRef> qualifiedNamesToRefs = HashMultimap.create();
        AlgOptPredicateList newPreds = AlgOptPredicateList.EMPTY;
        for ( AlgNode input : join.getInputs() ) {
            final AlgOptPredicateList inputPreds = mq.getAllPredicates( input );
            if ( inputPreds == null ) {
                // Bail out
                return null;
            }
            // Gather table references
            final Set<AlgTableRef> tableRefs = mq.getTableReferences( input );
            if ( input == join.getLeft() ) {
                // Left input references remain unchanged
                for ( AlgTableRef leftRef : tableRefs ) {
                    qualifiedNamesToRefs.put( leftRef.getQualifiedName(), leftRef );
                }
                newPreds = newPreds.union( rexBuilder, inputPreds );
            } else {
                // Right input references might need to be updated if there are table name clashes with left input
                final Map<AlgTableRef, AlgTableRef> currentTablesMapping = new HashMap<>();
                for ( AlgTableRef rightRef : tableRefs ) {
                    int shift = 0;
                    Collection<AlgTableRef> lRefs = qualifiedNamesToRefs.get( rightRef.getQualifiedName() );
                    if ( lRefs != null ) {
                        shift = lRefs.size();
                    }
                    currentTablesMapping.put( rightRef, AlgTableRef.of( rightRef.getTable(), shift + rightRef.getEntityNumber() ) );
                }
                final List<RexNode> updatedPreds = Lists.newArrayList( Iterables.transform( inputPreds.pulledUpPredicates, e -> RexUtil.swapTableReferences( rexBuilder, e, currentTablesMapping ) ) );
                newPreds = newPreds.union( rexBuilder, AlgOptPredicateList.of( rexBuilder, updatedPreds ) );
            }
        }

        // Extract input fields referenced by Join condition
        final Set<AlgDataTypeField> inputExtraFields = new LinkedHashSet<>();
        final AlgOptUtil.InputFinder inputFinder = new AlgOptUtil.InputFinder( inputExtraFields );
        pred.accept( inputFinder );
        final ImmutableBitSet inputFieldsUsed = inputFinder.inputBitSet.build();

        // Infer column origin expressions for given references
        final Map<RexIndexRef, Set<RexNode>> mapping = new LinkedHashMap<>();
        for ( int idx : inputFieldsUsed ) {
            final RexIndexRef inputRef = RexIndexRef.of( idx, join.getTupleType().getFields() );
            final Set<RexNode> originalExprs = mq.getExpressionLineage( join, inputRef );
            if ( originalExprs == null ) {
                // Bail out
                return null;
            }
            final RexIndexRef ref = RexIndexRef.of( idx, join.getTupleType().getFields() );
            mapping.put( ref, originalExprs );
        }

        // Replace with new expressions and return union of predicates
        final Set<RexNode> allExprs = AlgMdExpressionLineage.createAllPossibleExpressions( rexBuilder, pred, mapping );
        if ( allExprs == null ) {
            return null;
        }
        return newPreds.union( rexBuilder, AlgOptPredicateList.of( rexBuilder, allExprs ) );
    }


    /**
     * Extract predicates for an Aggregate.
     */
    public AlgOptPredicateList getAllPredicates( Aggregate agg, AlgMetadataQuery mq ) {
        return mq.getAllPredicates( agg.getInput() );
    }


    /**
     * Extract predicates for a Union.
     */
    public AlgOptPredicateList getAllPredicates( Union union, AlgMetadataQuery mq ) {
        final RexBuilder rexBuilder = union.getCluster().getRexBuilder();

        final Multimap<List<String>, AlgTableRef> qualifiedNamesToRefs = HashMultimap.create();
        AlgOptPredicateList newPreds = AlgOptPredicateList.EMPTY;
        for ( int i = 0; i < union.getInputs().size(); i++ ) {
            final AlgNode input = union.getInput( i );
            final AlgOptPredicateList inputPreds = mq.getAllPredicates( input );
            if ( inputPreds == null ) {
                // Bail out
                return null;
            }
            // Gather table references
            final Set<AlgTableRef> tableRefs = mq.getTableReferences( input );
            if ( i == 0 ) {
                // Left input references remain unchanged
                for ( AlgTableRef leftRef : tableRefs ) {
                    qualifiedNamesToRefs.put( leftRef.getQualifiedName(), leftRef );
                }
                newPreds = newPreds.union( rexBuilder, inputPreds );
            } else {
                // Right input references might need to be updated if there are table name
                // clashes with left input
                final Map<AlgTableRef, AlgTableRef> currentTablesMapping = new HashMap<>();
                for ( AlgTableRef rightRef : tableRefs ) {
                    int shift = 0;
                    Collection<AlgTableRef> lRefs = qualifiedNamesToRefs.get( rightRef.getQualifiedName() );
                    if ( lRefs != null ) {
                        shift = lRefs.size();
                    }
                    currentTablesMapping.put( rightRef, AlgTableRef.of( rightRef.getTable(), shift + rightRef.getEntityNumber() ) );
                }
                // Add to existing qualified names
                for ( AlgTableRef newRef : currentTablesMapping.values() ) {
                    qualifiedNamesToRefs.put( newRef.getQualifiedName(), newRef );
                }
                // Update preds
                final List<RexNode> updatedPreds = Lists.newArrayList( Iterables.transform( inputPreds.pulledUpPredicates, e -> RexUtil.swapTableReferences( rexBuilder, e, currentTablesMapping ) ) );
                newPreds = newPreds.union( rexBuilder, AlgOptPredicateList.of( rexBuilder, updatedPreds ) );
            }
        }
        return newPreds;
    }


    /**
     * Extract predicates for a Sort.
     */
    public AlgOptPredicateList getAllPredicates( Sort sort, AlgMetadataQuery mq ) {
        return mq.getAllPredicates( sort.getInput() );
    }


    /**
     * Extract predicates for an Exchange.
     */
    public AlgOptPredicateList getAllPredicates( Exchange exchange, AlgMetadataQuery mq ) {
        return mq.getAllPredicates( exchange.getInput() );
    }

}


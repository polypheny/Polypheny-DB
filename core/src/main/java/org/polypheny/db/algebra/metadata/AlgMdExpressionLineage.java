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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.Exchange;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.hep.HepAlgVertex;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexTableInputRef;
import org.polypheny.db.rex.RexTableInputRef.AlgTableRef;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Util;


/**
 * Default implementation of {@link AlgMetadataQuery#getExpressionLineage} for the standard logical algebra.
 *
 * The goal of this provider is to infer the lineage for the given expression.
 *
 * The output expressions might contain references to columns produced by {@link Scan} operators ({@link RexTableInputRef}). In turn, each Scan operator is identified uniquely
 * by a {@link AlgTableRef} containing its qualified name and an identifier.
 *
 * If the lineage cannot be inferred, we return null.
 */
public class AlgMdExpressionLineage implements MetadataHandler<BuiltInMetadata.ExpressionLineage> {

    public static final AlgMetadataProvider SOURCE = ReflectiveAlgMetadataProvider.reflectiveSource( BuiltInMethod.EXPRESSION_LINEAGE.method, new AlgMdExpressionLineage() );


    protected AlgMdExpressionLineage() {
    }


    @Override
    public MetadataDef<BuiltInMetadata.ExpressionLineage> getDef() {
        return BuiltInMetadata.ExpressionLineage.DEF;
    }


    // Catch-all rule when none of the others apply.
    public Set<RexNode> getExpressionLineage( AlgNode alg, AlgMetadataQuery mq, RexNode outputExpression ) {
        return null;
    }


    public Set<RexNode> getExpressionLineage( HepAlgVertex alg, AlgMetadataQuery mq, RexNode outputExpression ) {
        return mq.getExpressionLineage( alg.getCurrentAlg(), outputExpression );
    }


    public Set<RexNode> getExpressionLineage( AlgSubset alg, AlgMetadataQuery mq, RexNode outputExpression ) {
        return mq.getExpressionLineage( Util.first( alg.getBest(), alg.getOriginal() ), outputExpression );
    }


    /**
     * Expression lineage from {@link Scan}.
     *
     * We extract the fields referenced by the expression and we express them using {@link RexTableInputRef}.
     */
    public Set<RexNode> getExpressionLineage( Scan alg, AlgMetadataQuery mq, RexNode outputExpression ) {
        final RexBuilder rexBuilder = alg.getCluster().getRexBuilder();

        // Extract input fields referenced by expression
        final ImmutableBitSet inputFieldsUsed = extractInputRefs( outputExpression );

        // Infer column origin expressions for given references
        final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
        for ( int idx : inputFieldsUsed ) {
            final RexNode inputRef = RexTableInputRef.of(
                    AlgTableRef.of( alg.getTable(), 0 ),
                    RexInputRef.of( idx, alg.getRowType().getFieldList() ) );
            final RexInputRef ref = RexInputRef.of( idx, alg.getRowType().getFieldList() );
            mapping.put( ref, ImmutableSet.of( inputRef ) );
        }

        // Return result
        return createAllPossibleExpressions( rexBuilder, outputExpression, mapping );
    }


    /**
     * Expression lineage from {@link Aggregate}.
     *
     * If the expression references grouping sets or aggregate function results, we cannot extract the lineage and we return null.
     */
    public Set<RexNode> getExpressionLineage( Aggregate alg, AlgMetadataQuery mq, RexNode outputExpression ) {
        final AlgNode input = alg.getInput();
        final RexBuilder rexBuilder = alg.getCluster().getRexBuilder();

        // Extract input fields referenced by expression
        final ImmutableBitSet inputFieldsUsed = extractInputRefs( outputExpression );

        for ( int idx : inputFieldsUsed ) {
            if ( idx >= alg.getGroupCount() ) {
                // We cannot map origin of this expression.
                return null;
            }
        }

        // Infer column origin expressions for given references
        final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
        for ( int idx : inputFieldsUsed ) {
            final RexInputRef inputRef = RexInputRef.of( alg.getGroupSet().nth( idx ), input.getRowType().getFieldList() );
            final Set<RexNode> originalExprs = mq.getExpressionLineage( input, inputRef );
            if ( originalExprs == null ) {
                // Bail out
                return null;
            }
            final RexInputRef ref = RexInputRef.of( idx, alg.getRowType().getFieldList() );
            mapping.put( ref, originalExprs );
        }

        // Return result
        return createAllPossibleExpressions( rexBuilder, outputExpression, mapping );
    }


    /**
     * Expression lineage from {@link Join}.
     *
     * We only extract the lineage for INNER joins.
     */
    public Set<RexNode> getExpressionLineage( Join alg, AlgMetadataQuery mq, RexNode outputExpression ) {
        final RexBuilder rexBuilder = alg.getCluster().getRexBuilder();
        final AlgNode leftInput = alg.getLeft();
        final AlgNode rightInput = alg.getRight();
        final int nLeftColumns = leftInput.getRowType().getFieldList().size();

        // Extract input fields referenced by expression
        final ImmutableBitSet inputFieldsUsed = extractInputRefs( outputExpression );

        if ( alg.getJoinType() != JoinAlgType.INNER ) {
            // If we reference the inner side, we will bail out
            if ( alg.getJoinType() == JoinAlgType.LEFT ) {
                ImmutableBitSet rightFields = ImmutableBitSet.range( nLeftColumns, alg.getRowType().getFieldCount() );
                if ( inputFieldsUsed.intersects( rightFields ) ) {
                    // We cannot map origin of this expression.
                    return null;
                }
            } else if ( alg.getJoinType() == JoinAlgType.RIGHT ) {
                ImmutableBitSet leftFields = ImmutableBitSet.range( 0, nLeftColumns );
                if ( inputFieldsUsed.intersects( leftFields ) ) {
                    // We cannot map origin of this expression.
                    return null;
                }
            } else {
                // We cannot map origin of this expression.
                return null;
            }
        }

        // Gather table references
        final Set<AlgTableRef> leftTableRefs = mq.getTableReferences( leftInput );
        if ( leftTableRefs == null ) {
            // Bail out
            return null;
        }
        final Set<AlgTableRef> rightTableRefs = mq.getTableReferences( rightInput );
        if ( rightTableRefs == null ) {
            // Bail out
            return null;
        }
        final Multimap<List<String>, AlgTableRef> qualifiedNamesToRefs = HashMultimap.create();
        final Map<AlgTableRef, AlgTableRef> currentTablesMapping = new HashMap<>();
        for ( AlgTableRef leftRef : leftTableRefs ) {
            qualifiedNamesToRefs.put( leftRef.getQualifiedName(), leftRef );
        }
        for ( AlgTableRef rightRef : rightTableRefs ) {
            int shift = 0;
            Collection<AlgTableRef> lRefs = qualifiedNamesToRefs.get( rightRef.getQualifiedName() );
            if ( lRefs != null ) {
                shift = lRefs.size();
            }
            currentTablesMapping.put( rightRef, AlgTableRef.of( rightRef.getTable(), shift + rightRef.getEntityNumber() ) );
        }

        // Infer column origin expressions for given references
        final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
        for ( int idx : inputFieldsUsed ) {
            if ( idx < nLeftColumns ) {
                final RexInputRef inputRef = RexInputRef.of( idx, leftInput.getRowType().getFieldList() );
                final Set<RexNode> originalExprs = mq.getExpressionLineage( leftInput, inputRef );
                if ( originalExprs == null ) {
                    // Bail out
                    return null;
                }
                // Left input references remain unchanged
                mapping.put( RexInputRef.of( idx, alg.getRowType().getFieldList() ), originalExprs );
            } else {
                // Right input.
                final RexInputRef inputRef = RexInputRef.of( idx - nLeftColumns, rightInput.getRowType().getFieldList() );
                final Set<RexNode> originalExprs = mq.getExpressionLineage( rightInput, inputRef );
                if ( originalExprs == null ) {
                    // Bail out
                    return null;
                }
                // Right input references might need to be updated if there are table names clashes with left input
                final Set<RexNode> updatedExprs = ImmutableSet.copyOf( Iterables.transform( originalExprs, e -> RexUtil.swapTableReferences( rexBuilder, e, currentTablesMapping ) ) );
                mapping.put( RexInputRef.of( idx, alg.getRowType().getFieldList() ), updatedExprs );
            }
        }

        // Return result
        return createAllPossibleExpressions( rexBuilder, outputExpression, mapping );
    }


    /**
     * Expression lineage from {@link Union}.
     *
     * For Union operator, we might be able to extract multiple origins for the references in the given expression.
     */
    public Set<RexNode> getExpressionLineage( Union alg, AlgMetadataQuery mq, RexNode outputExpression ) {
        final RexBuilder rexBuilder = alg.getCluster().getRexBuilder();

        // Extract input fields referenced by expression
        final ImmutableBitSet inputFieldsUsed = extractInputRefs( outputExpression );

        // Infer column origin expressions for given references
        final Multimap<List<String>, AlgTableRef> qualifiedNamesToRefs = HashMultimap.create();
        final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
        for ( AlgNode input : alg.getInputs() ) {
            // Gather table references
            final Map<AlgTableRef, AlgTableRef> currentTablesMapping = new HashMap<>();
            final Set<AlgTableRef> tableRefs = mq.getTableReferences( input );
            if ( tableRefs == null ) {
                // Bail out
                return null;
            }
            for ( AlgTableRef tableRef : tableRefs ) {
                int shift = 0;
                Collection<AlgTableRef> lRefs = qualifiedNamesToRefs.get( tableRef.getQualifiedName() );
                if ( lRefs != null ) {
                    shift = lRefs.size();
                }
                currentTablesMapping.put( tableRef, AlgTableRef.of( tableRef.getTable(), shift + tableRef.getEntityNumber() ) );
            }
            // Map references
            for ( int idx : inputFieldsUsed ) {
                final RexInputRef inputRef = RexInputRef.of( idx, input.getRowType().getFieldList() );
                final Set<RexNode> originalExprs = mq.getExpressionLineage( input, inputRef );
                if ( originalExprs == null ) {
                    // Bail out
                    return null;
                }
                // References might need to be updated
                final RexInputRef ref = RexInputRef.of( idx, alg.getRowType().getFieldList() );
                final Set<RexNode> updatedExprs =
                        originalExprs.stream()
                                .map( e -> RexUtil.swapTableReferences( rexBuilder, e, currentTablesMapping ) )
                                .collect( Collectors.toSet() );
                final Set<RexNode> set = mapping.get( ref );
                if ( set != null ) {
                    set.addAll( updatedExprs );
                } else {
                    mapping.put( ref, updatedExprs );
                }
            }
            // Add to existing qualified names
            for ( AlgTableRef newRef : currentTablesMapping.values() ) {
                qualifiedNamesToRefs.put( newRef.getQualifiedName(), newRef );
            }
        }

        // Return result
        return createAllPossibleExpressions( rexBuilder, outputExpression, mapping );
    }


    /**
     * Expression lineage from Project.
     */
    public Set<RexNode> getExpressionLineage( Project alg, final AlgMetadataQuery mq, RexNode outputExpression ) {
        final AlgNode input = alg.getInput();
        final RexBuilder rexBuilder = alg.getCluster().getRexBuilder();

        // Extract input fields referenced by expression
        final ImmutableBitSet inputFieldsUsed = extractInputRefs( outputExpression );

        // Infer column origin expressions for given references
        final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
        for ( int idx : inputFieldsUsed ) {
            final RexNode inputExpr = alg.getChildExps().get( idx );
            final Set<RexNode> originalExprs = mq.getExpressionLineage( input, inputExpr );
            if ( originalExprs == null ) {
                // Bail out
                return null;
            }
            final RexInputRef ref = RexInputRef.of( idx, alg.getRowType().getFieldList() );
            mapping.put( ref, originalExprs );
        }

        // Return result
        return createAllPossibleExpressions( rexBuilder, outputExpression, mapping );
    }


    /**
     * Expression lineage from Filter.
     */
    public Set<RexNode> getExpressionLineage( Filter alg, AlgMetadataQuery mq, RexNode outputExpression ) {
        return mq.getExpressionLineage( alg.getInput(), outputExpression );
    }


    /**
     * Expression lineage from Sort.
     */
    public Set<RexNode> getExpressionLineage( Sort alg, AlgMetadataQuery mq, RexNode outputExpression ) {
        return mq.getExpressionLineage( alg.getInput(), outputExpression );
    }


    /**
     * Expression lineage from Exchange.
     */
    public Set<RexNode> getExpressionLineage( Exchange alg, AlgMetadataQuery mq, RexNode outputExpression ) {
        return mq.getExpressionLineage( alg.getInput(), outputExpression );
    }


    /**
     * Given an expression, it will create all equivalent expressions resulting from replacing all possible combinations of references in the mapping by the corresponding expressions.
     *
     * @param rexBuilder rexBuilder
     * @param expr expression
     * @param mapping mapping
     * @return set of resulting expressions equivalent to the input expression
     */
    @Nullable
    protected static Set<RexNode> createAllPossibleExpressions( RexBuilder rexBuilder, RexNode expr, Map<RexInputRef, Set<RexNode>> mapping ) {
        // Extract input fields referenced by expression
        final ImmutableBitSet predFieldsUsed = extractInputRefs( expr );

        if ( predFieldsUsed.isEmpty() ) {
            // The unique expression is the input expression
            return ImmutableSet.of( expr );
        }

        try {
            return createAllPossibleExpressions( rexBuilder, expr, predFieldsUsed, mapping, new HashMap<>() );
        } catch ( UnsupportedOperationException e ) {
            // There may be a RexNode unsupported by RexCopier, just return null
            return null;
        }
    }


    private static Set<RexNode> createAllPossibleExpressions( RexBuilder rexBuilder, RexNode expr, ImmutableBitSet predFieldsUsed, Map<RexInputRef, Set<RexNode>> mapping, Map<RexInputRef, RexNode> singleMapping ) {
        final RexInputRef inputRef = mapping.keySet().iterator().next();
        final Set<RexNode> replacements = mapping.remove( inputRef );
        Set<RexNode> result = new HashSet<>();
        assert !replacements.isEmpty();
        if ( predFieldsUsed.indexOf( inputRef.getIndex() ) != -1 ) {
            for ( RexNode replacement : replacements ) {
                singleMapping.put( inputRef, replacement );
                createExpressions( rexBuilder, expr, predFieldsUsed, mapping, singleMapping, result );
                singleMapping.remove( inputRef );
            }
        } else {
            createExpressions( rexBuilder, expr, predFieldsUsed, mapping, singleMapping, result );
        }
        mapping.put( inputRef, replacements );
        return result;
    }


    private static void createExpressions( RexBuilder rexBuilder, RexNode expr, ImmutableBitSet predFieldsUsed, Map<RexInputRef, Set<RexNode>> mapping, Map<RexInputRef, RexNode> singleMapping, Set<RexNode> result ) {
        if ( mapping.isEmpty() ) {
            final RexReplacer replacer = new RexReplacer( singleMapping );
            final List<RexNode> updatedPreds = new ArrayList<>( AlgOptUtil.conjunctions( rexBuilder.copy( expr ) ) );
            replacer.mutate( updatedPreds );
            result.addAll( updatedPreds );
        } else {
            result.addAll( createAllPossibleExpressions( rexBuilder, expr, predFieldsUsed, mapping, singleMapping ) );
        }
    }


    /**
     * Replaces expressions with their equivalences. Note that we only have to look for RexInputRef.
     */
    private static class RexReplacer extends RexShuttle {

        private final Map<RexInputRef, RexNode> replacementValues;


        RexReplacer( Map<RexInputRef, RexNode> replacementValues ) {
            this.replacementValues = replacementValues;
        }


        @Override
        public RexNode visitInputRef( RexInputRef inputRef ) {
            return replacementValues.get( inputRef );
        }

    }


    private static ImmutableBitSet extractInputRefs( RexNode expr ) {
        final Set<AlgDataTypeField> inputExtraFields = new LinkedHashSet<>();
        final AlgOptUtil.InputFinder inputFinder = new AlgOptUtil.InputFinder( inputExtraFields );
        expr.accept( inputFinder );
        return inputFinder.inputBitSet.build();
    }

}


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

package org.polypheny.db.algebra;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.core.SemiJoin;
import org.polypheny.db.algebra.core.SetOp;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.logical.relational.LogicalAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalFilter;
import org.polypheny.db.algebra.logical.relational.LogicalJoin;
import org.polypheny.db.algebra.logical.relational.LogicalModify;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.logical.relational.LogicalScan;
import org.polypheny.db.algebra.logical.relational.LogicalTableFunctionScan;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCorrelVariable;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexPermuteInputsShuttle;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.rex.RexVisitor;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.util.Bug;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.ReflectUtil;
import org.polypheny.db.util.ReflectiveVisitor;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.mapping.IntPair;
import org.polypheny.db.util.mapping.Mapping;
import org.polypheny.db.util.mapping.MappingType;
import org.polypheny.db.util.mapping.Mappings;
import org.slf4j.Logger;


/**
 * Transformer that walks over a tree of relational expressions, replacing each {@link AlgNode} with a 'slimmed down' relational expression that projects only the columns required by its consumer.
 *
 * Uses multi-methods to fire the right rule for each type of relational expression. This allows the transformer to be extended without having to add a new method to AlgNode,
 * and without requiring a collection of rule classes scattered to the four winds.
 *
 * REVIEW: jhyde: Is sql2rel the correct package for this class? Trimming fields is not an essential part of SQL-to-Rel translation, and arguably belongs in the optimization phase. But this transformer does not
 * obey the usual pattern for planner rules; it is difficult to do so, because each {@link AlgNode} needs to return a different set of fields after trimming.
 *
 * TODO: Change 2nd arg of the {@link #trimFields} method from BitSet to Mapping. Sometimes it helps the consumer if you return the columns in a particular order. For instance, it may avoid a project at the top of the tree just
 * for reordering. Could ease the transition by writing methods that convert BitSet to Mapping and vice versa.
 */
public class AlgFieldTrimmer implements ReflectiveVisitor {

    private final ReflectUtil.MethodDispatcher<TrimResult> trimFieldsDispatcher;
    private final AlgBuilder algBuilder;


    /**
     * Creates a AlgFieldTrimmer.
     *
     * @param validator Validator
     */
    public AlgFieldTrimmer( Validator validator, AlgBuilder algBuilder ) {
        Util.discard( validator ); // may be useful one day
        this.algBuilder = algBuilder;
        this.trimFieldsDispatcher =
                ReflectUtil.createMethodDispatcher(
                        TrimResult.class,
                        this,
                        "trimFields",
                        AlgNode.class,
                        ImmutableBitSet.class,
                        Set.class );
    }


    /**
     * Trims unused fields from a relational expression.
     *
     * We presume that all fields of the relational expression are wanted by its consumer, so only trim fields that are not used within the tree.
     *
     * @param root Root node of relational expression
     * @return Trimmed relational expression
     */
    public AlgNode trim( AlgNode root ) {
        final int fieldCount = root.getRowType().getFieldCount();
        final ImmutableBitSet fieldsUsed = ImmutableBitSet.range( fieldCount );
        final Set<AlgDataTypeField> extraFields = Collections.emptySet();
        final TrimResult trimResult = dispatchTrimFields( root, fieldsUsed, extraFields );
        if ( !trimResult.right.isIdentity() ) {
            throw new IllegalArgumentException();
        }
        Logger logger = LanguageManager.getInstance().getLogger( QueryLanguage.SQL, AlgNode.class );
        if ( logger.isDebugEnabled() ) {
            logger.debug(
                    AlgOptUtil.dumpPlan(
                            "Plan after trimming unused fields",
                            trimResult.left,
                            ExplainFormat.TEXT,
                            ExplainLevel.EXPPLAN_ATTRIBUTES ) );
        }
        return trimResult.left;
    }


    /**
     * Trims the fields of an input relational expression.
     *
     * @param alg Relational expression
     * @param input Input relational expression, whose fields to trim
     * @param fieldsUsed Bitmap of fields needed by the consumer
     * @return New relational expression and its field mapping
     */
    protected TrimResult trimChild( AlgNode alg, AlgNode input, final ImmutableBitSet fieldsUsed, Set<AlgDataTypeField> extraFields ) {
        final ImmutableBitSet.Builder fieldsUsedBuilder = fieldsUsed.rebuild();

        // Fields that define the collation cannot be discarded.
        final AlgMetadataQuery mq = alg.getCluster().getMetadataQuery();
        final ImmutableList<AlgCollation> collations = mq.collations( input );
        for ( AlgCollation collation : collations ) {
            for ( AlgFieldCollation fieldCollation : collation.getFieldCollations() ) {
                fieldsUsedBuilder.set( fieldCollation.getFieldIndex() );
            }
        }

        // Correlating variables are a means for other relational expressions to use fields.
        for ( final CorrelationId correlation : alg.getVariablesSet() ) {
            alg.accept(
                    new CorrelationReferenceFinder() {
                        @Override
                        protected RexNode handle( RexFieldAccess fieldAccess ) {
                            final RexCorrelVariable v = (RexCorrelVariable) fieldAccess.getReferenceExpr();
                            if ( v.id.equals( correlation ) ) {
                                fieldsUsedBuilder.set( fieldAccess.getField().getIndex() );
                            }
                            return fieldAccess;
                        }
                    } );
        }

        return dispatchTrimFields( input, fieldsUsedBuilder.build(), extraFields );
    }


    /**
     * Trims a child relational expression, then adds back a dummy project to restore the fields that were removed.
     *
     * Sounds pointless? It causes unused fields to be removed further down the tree (towards the leaves), but it ensure that the consuming relational expression continues to see the same fields.
     *
     * @param alg Relational expression
     * @param input Input relational expression, whose fields to trim
     * @param fieldsUsed Bitmap of fields needed by the consumer
     * @return New relational expression and its field mapping
     */
    protected TrimResult trimChildRestore( AlgNode alg, AlgNode input, ImmutableBitSet fieldsUsed, Set<AlgDataTypeField> extraFields ) {
        TrimResult trimResult = trimChild( alg, input, fieldsUsed, extraFields );
        if ( trimResult.right.isIdentity() ) {
            return trimResult;
        }
        final AlgDataType rowType = input.getRowType();
        List<AlgDataTypeField> fieldList = rowType.getFieldList();
        final List<RexNode> exprList = new ArrayList<>();
        final List<String> nameList = rowType.getFieldNames();
        RexBuilder rexBuilder = alg.getCluster().getRexBuilder();
        assert trimResult.right.getSourceCount() == fieldList.size();
        for ( int i = 0; i < fieldList.size(); i++ ) {
            int source = trimResult.right.getTargetOpt( i );
            AlgDataTypeField field = fieldList.get( i );
            exprList.add(
                    source < 0
                            ? rexBuilder.makeZeroLiteral( field.getType() )
                            : rexBuilder.makeInputRef( field.getType(), source ) );
        }
        algBuilder.push( trimResult.left ).project( exprList, nameList );
        return result( algBuilder.build(), Mappings.createIdentity( fieldList.size() ) );
    }


    /**
     * Invokes {@link #trimFields}, or the appropriate method for the type of the alg parameter, using multi-method dispatch.
     *
     * @param alg Relational expression
     * @param fieldsUsed Bitmap of fields needed by the consumer
     * @return New relational expression and its field mapping
     */
    protected final TrimResult dispatchTrimFields( AlgNode alg, ImmutableBitSet fieldsUsed, Set<AlgDataTypeField> extraFields ) {
        final TrimResult trimResult = trimFieldsDispatcher.invoke( alg, fieldsUsed, extraFields );
        final AlgNode newRel = trimResult.left;
        final Mapping mapping = trimResult.right;
        final int fieldCount = alg.getRowType().getFieldCount();
        assert mapping.getSourceCount() == fieldCount : "source: " + mapping.getSourceCount() + " != " + fieldCount;
        final int newFieldCount = newRel.getRowType().getFieldCount();
        assert mapping.getTargetCount() + extraFields.size() == newFieldCount || Bug.TODO_FIXED
                : "target: " + mapping.getTargetCount() + " + " + extraFields.size() + " != " + newFieldCount;
        if ( Bug.TODO_FIXED ) {
            assert newFieldCount > 0 : "rel has no fields after trim: " + alg;
        }
        if ( newRel.equals( alg ) ) {
            return result( alg, mapping );
        }
        return trimResult;
    }


    protected TrimResult result( AlgNode r, final Mapping mapping ) {
        final RexBuilder rexBuilder = algBuilder.getRexBuilder();
        for ( final CorrelationId correlation : r.getVariablesSet() ) {
            r = r.accept(
                    new CorrelationReferenceFinder() {
                        @Override
                        protected RexNode handle( RexFieldAccess fieldAccess ) {
                            final RexCorrelVariable v = (RexCorrelVariable) fieldAccess.getReferenceExpr();
                            if ( v.id.equals( correlation ) && v.getType().getFieldCount() == mapping.getSourceCount() ) {
                                final int old = fieldAccess.getField().getIndex();
                                final int new_ = mapping.getTarget( old );
                                final AlgDataTypeFactory.Builder typeBuilder = algBuilder.getTypeFactory().builder();
                                for ( int target : Util.range( mapping.getTargetCount() ) ) {
                                    typeBuilder.add( v.getType().getFieldList().get( mapping.getSource( target ) ) );
                                }
                                final RexNode newV = rexBuilder.makeCorrel( typeBuilder.build(), v.id );
                                if ( old != new_ ) {
                                    return rexBuilder.makeFieldAccess( newV, new_ );
                                }
                            }
                            return fieldAccess;
                        }
                    } );
        }
        return new TrimResult( r, mapping );
    }


    /**
     * Visit method, per {@link org.polypheny.db.util.ReflectiveVisitor}.
     *
     * This method is invoked reflectively, so there may not be any apparent calls to it. The class (or derived classes) may contain overloads of this method with more specific types for the {@code rel} parameter.
     *
     * Returns a pair: the relational expression created, and the mapping between the original fields and the fields of the newly created relational expression.
     *
     * @param alg Relational expression
     * @param fieldsUsed Fields needed by the consumer
     * @return relational expression and mapping
     */
    public TrimResult trimFields( AlgNode alg, ImmutableBitSet fieldsUsed, Set<AlgDataTypeField> extraFields ) {
        // We don't know how to trim this kind of relational expression, so give it back intact.
        Util.discard( fieldsUsed );
        return result( alg, Mappings.createIdentity( alg.getRowType().getFieldCount() ) );
    }


    /**
     * Variant of {@link #trimFields(AlgNode, ImmutableBitSet, Set)} for {@link LogicalProject}.
     */
    public TrimResult trimFields( Project project, ImmutableBitSet fieldsUsed, Set<AlgDataTypeField> extraFields ) {
        final AlgDataType rowType = project.getRowType();
        final int fieldCount = rowType.getFieldCount();
        final AlgNode input = project.getInput();

        // Which fields are required from the input?
        final Set<AlgDataTypeField> inputExtraFields = new LinkedHashSet<>( extraFields );
        AlgOptUtil.InputFinder inputFinder = new AlgOptUtil.InputFinder( inputExtraFields );
        for ( Ord<RexNode> ord : Ord.zip( project.getProjects() ) ) {
            if ( fieldsUsed.get( ord.i ) ) {
                ord.e.accept( inputFinder );
            }
        }
        ImmutableBitSet inputFieldsUsed = inputFinder.inputBitSet.build();

        // Create input with trimmed columns.
        TrimResult trimResult = trimChild( project, input, inputFieldsUsed, inputExtraFields );
        AlgNode newInput = trimResult.left;
        final Mapping inputMapping = trimResult.right;

        // If the input is unchanged, and we need to project all columns, there's nothing we can do.
        if ( newInput == input && fieldsUsed.cardinality() == fieldCount ) {
            return result( project, Mappings.createIdentity( fieldCount ) );
        }

        // Some parts of the system can't handle rows with zero fields, so pretend that one field is used.
        if ( fieldsUsed.cardinality() == 0 ) {
            return dummyProject( fieldCount, newInput );
        }

        // Build new project expressions, and populate the mapping.
        final List<RexNode> newProjects = new ArrayList<>();
        final RexVisitor<RexNode> shuttle = new RexPermuteInputsShuttle( inputMapping, newInput );
        final Mapping mapping =
                Mappings.create(
                        MappingType.INVERSE_SURJECTION,
                        fieldCount,
                        fieldsUsed.cardinality() );
        for ( Ord<RexNode> ord : Ord.zip( project.getProjects() ) ) {
            if ( fieldsUsed.get( ord.i ) ) {
                mapping.set( ord.i, newProjects.size() );
                RexNode newProjectExpr = ord.e.accept( shuttle );
                newProjects.add( newProjectExpr );
            }
        }

        final AlgDataType newRowType = AlgOptUtil.permute( project.getCluster().getTypeFactory(), rowType, mapping );

        algBuilder.push( newInput );
        algBuilder.project( newProjects, newRowType.getFieldNames() );
        return result( algBuilder.build(), mapping );
    }


    /**
     * Creates a project with a dummy column, to protect the parts of the system that cannot handle a relational expression with no columns.
     *
     * @param fieldCount Number of fields in the original relational expression
     * @param input Trimmed input
     * @return Dummy project, or null if no dummy is required
     */
    protected TrimResult dummyProject( int fieldCount, AlgNode input ) {
        final AlgOptCluster cluster = input.getCluster();
        final Mapping mapping = Mappings.create( MappingType.INVERSE_SURJECTION, fieldCount, 1 );
        if ( input.getRowType().getFieldCount() == 1 ) {
            // Input already has one field (and may in fact be a dummy project we created for the child). We can't do better.
            return result( input, mapping );
        }
        final RexLiteral expr = cluster.getRexBuilder().makeExactLiteral( BigDecimal.ZERO );
        algBuilder.push( input );
        algBuilder.project( ImmutableList.<RexNode>of( expr ), ImmutableList.of( "DUMMY" ) );
        return result( algBuilder.build(), mapping );
    }


    /**
     * Variant of {@link #trimFields(AlgNode, ImmutableBitSet, Set)} for {@link LogicalFilter}.
     */
    public TrimResult trimFields( Filter filter, ImmutableBitSet fieldsUsed, Set<AlgDataTypeField> extraFields ) {
        final AlgDataType rowType = filter.getRowType();
        final int fieldCount = rowType.getFieldCount();
        final RexNode conditionExpr = filter.getCondition();
        final AlgNode input = filter.getInput();

        // We use the fields used by the consumer, plus any fields used in the filter.
        final Set<AlgDataTypeField> inputExtraFields = new LinkedHashSet<>( extraFields );
        AlgOptUtil.InputFinder inputFinder = new AlgOptUtil.InputFinder( inputExtraFields );
        inputFinder.inputBitSet.addAll( fieldsUsed );
        conditionExpr.accept( inputFinder );
        final ImmutableBitSet inputFieldsUsed = inputFinder.inputBitSet.build();

        // Create input with trimmed columns.
        TrimResult trimResult = trimChild( filter, input, inputFieldsUsed, inputExtraFields );
        AlgNode newInput = trimResult.left;
        final Mapping inputMapping = trimResult.right;

        // If the input is unchanged, and we need to project all columns, there's nothing we can do.
        if ( newInput == input && fieldsUsed.cardinality() == fieldCount ) {
            return result( filter, Mappings.createIdentity( fieldCount ) );
        }

        // Build new project expressions, and populate the mapping.
        final RexVisitor<RexNode> shuttle = new RexPermuteInputsShuttle( inputMapping, newInput );
        RexNode newConditionExpr = conditionExpr.accept( shuttle );

        // Use copy rather than algBuilder so that correlating variables get set.
        algBuilder.push( filter.copy( filter.getTraitSet(), newInput, newConditionExpr ) );

        // The result has the same mapping as the input gave us. Sometimes we return fields that the consumer didn't ask for, because the filter needs them for its condition.
        return result( algBuilder.build(), inputMapping );
    }


    /**
     * Variant of {@link #trimFields(AlgNode, ImmutableBitSet, Set)} for {@link Sort}.
     */
    public TrimResult trimFields( Sort sort, ImmutableBitSet fieldsUsed, Set<AlgDataTypeField> extraFields ) {
        final AlgDataType rowType = sort.getRowType();
        final int fieldCount = rowType.getFieldCount();
        final AlgCollation collation = sort.getCollation();
        final AlgNode input = sort.getInput();

        // We use the fields used by the consumer, plus any fields used as sort keys.
        final ImmutableBitSet.Builder inputFieldsUsed = fieldsUsed.rebuild();
        for ( AlgFieldCollation field : collation.getFieldCollations() ) {
            inputFieldsUsed.set( field.getFieldIndex() );
        }

        // Create input with trimmed columns.
        final Set<AlgDataTypeField> inputExtraFields = Collections.emptySet();
        TrimResult trimResult = trimChild( sort, input, inputFieldsUsed.build(), inputExtraFields );
        AlgNode newInput = trimResult.left;
        final Mapping inputMapping = trimResult.right;

        // If the input is unchanged, and we need to project all columns, there's nothing we can do.
        if ( newInput == input && inputMapping.isIdentity() && fieldsUsed.cardinality() == fieldCount ) {
            return result( sort, Mappings.createIdentity( fieldCount ) );
        }

        // leave the Sort unchanged in case we have dynamic limits
        if ( sort.offset instanceof RexDynamicParam || sort.fetch instanceof RexDynamicParam ) {
            return result( sort, inputMapping );
        }

        algBuilder.push( newInput );
        final int offset =
                sort.offset == null
                        ? 0
                        : RexLiteral.intValue( sort.offset );
        final int fetch =
                sort.fetch == null
                        ? -1
                        : RexLiteral.intValue( sort.fetch );
        final ImmutableList<RexNode> fields = algBuilder.fields( RexUtil.apply( inputMapping, collation ) );
        algBuilder.sortLimit( offset, fetch, fields );

        // The result has the same mapping as the input gave us. Sometimes we return fields that the consumer didn't ask for, because the filter needs them for its condition.
        return result( algBuilder.build(), inputMapping );
    }


    /**
     * Variant of {@link #trimFields(AlgNode, ImmutableBitSet, Set)} for {@link LogicalJoin}.
     */
    public TrimResult trimFields( Join join, ImmutableBitSet fieldsUsed, Set<AlgDataTypeField> extraFields ) {
        final int fieldCount = join.getSystemFieldList().size() + join.getLeft().getRowType().getFieldCount() + join.getRight().getRowType().getFieldCount();
        final RexNode conditionExpr = join.getCondition();
        final int systemFieldCount = join.getSystemFieldList().size();

        // Add in fields used in the condition.
        final Set<AlgDataTypeField> combinedInputExtraFields = new LinkedHashSet<>( extraFields );
        AlgOptUtil.InputFinder inputFinder = new AlgOptUtil.InputFinder( combinedInputExtraFields );
        inputFinder.inputBitSet.addAll( fieldsUsed );
        conditionExpr.accept( inputFinder );
        final ImmutableBitSet fieldsUsedPlus = inputFinder.inputBitSet.build();

        // If no system fields are used, we can remove them.
        int systemFieldUsedCount = 0;
        for ( int i = 0; i < systemFieldCount; ++i ) {
            if ( fieldsUsed.get( i ) ) {
                ++systemFieldUsedCount;
            }
        }
        final int newSystemFieldCount;
        if ( systemFieldUsedCount == 0 ) {
            newSystemFieldCount = 0;
        } else {
            newSystemFieldCount = systemFieldCount;
        }

        int offset = systemFieldCount;
        int changeCount = 0;
        int newFieldCount = newSystemFieldCount;
        final List<AlgNode> newInputs = new ArrayList<>( 2 );
        final List<Mapping> inputMappings = new ArrayList<>();
        final List<Integer> inputExtraFieldCounts = new ArrayList<>();
        for ( AlgNode input : join.getInputs() ) {
            final AlgDataType inputRowType = input.getRowType();
            final int inputFieldCount = inputRowType.getFieldCount();

            // Compute required mapping.
            ImmutableBitSet.Builder inputFieldsUsed = ImmutableBitSet.builder();
            for ( int bit : fieldsUsedPlus ) {
                if ( bit >= offset && bit < offset + inputFieldCount ) {
                    inputFieldsUsed.set( bit - offset );
                }
            }

            // If there are system fields, we automatically use the corresponding field in each input.
            inputFieldsUsed.set( 0, newSystemFieldCount );

            // FIXME: We ought to collect extra fields for each input individually. For now, we assume that just one input has on-demand fields.
            Set<AlgDataTypeField> inputExtraFields =
                    AlgDataTypeImpl.extra( inputRowType ) == null
                            ? Collections.emptySet()
                            : combinedInputExtraFields;
            inputExtraFieldCounts.add( inputExtraFields.size() );
            TrimResult trimResult = trimChild( join, input, inputFieldsUsed.build(), inputExtraFields );
            newInputs.add( trimResult.left );
            if ( trimResult.left != input ) {
                ++changeCount;
            }

            final Mapping inputMapping = trimResult.right;
            inputMappings.add( inputMapping );

            // Move offset to point to start of next input.
            offset += inputFieldCount;
            newFieldCount += inputMapping.getTargetCount() + inputExtraFields.size();
        }

        Mapping mapping = Mappings.create( MappingType.INVERSE_SURJECTION, fieldCount, newFieldCount );
        for ( int i = 0; i < newSystemFieldCount; ++i ) {
            mapping.set( i, i );
        }
        offset = systemFieldCount;
        int newOffset = newSystemFieldCount;
        for ( int i = 0; i < inputMappings.size(); i++ ) {
            Mapping inputMapping = inputMappings.get( i );
            for ( IntPair pair : inputMapping ) {
                mapping.set( pair.source + offset, pair.target + newOffset );
            }
            offset += inputMapping.getSourceCount();
            newOffset += inputMapping.getTargetCount() + inputExtraFieldCounts.get( i );
        }

        if ( changeCount == 0 && mapping.isIdentity() ) {
            return result( join, Mappings.createIdentity( fieldCount ) );
        }

        // Build new join.
        final RexVisitor<RexNode> shuttle = new RexPermuteInputsShuttle( mapping, newInputs.get( 0 ), newInputs.get( 1 ) );
        RexNode newConditionExpr = conditionExpr.accept( shuttle );

        algBuilder.push( newInputs.get( 0 ) );
        algBuilder.push( newInputs.get( 1 ) );

        if ( join instanceof SemiJoin ) {
            algBuilder.semiJoin( newConditionExpr );
            // For SemiJoins only map fields from the left-side
            Mapping inputMapping = inputMappings.get( 0 );
            mapping = Mappings.create(
                    MappingType.INVERSE_SURJECTION,
                    join.getRowType().getFieldCount(),
                    newSystemFieldCount + inputMapping.getTargetCount() );
            for ( int i = 0; i < newSystemFieldCount; ++i ) {
                mapping.set( i, i );
            }
            offset = systemFieldCount;
            newOffset = newSystemFieldCount;
            for ( IntPair pair : inputMapping ) {
                mapping.set( pair.source + offset, pair.target + newOffset );
            }
        } else {
            algBuilder.join( join.getJoinType(), newConditionExpr );
        }

        return result( algBuilder.build(), mapping );
    }


    /**
     * Variant of {@link #trimFields(AlgNode, ImmutableBitSet, Set)} for {@link org.polypheny.db.algebra.core.SetOp} (including UNION and UNION ALL).
     */
    public TrimResult trimFields( SetOp setOp, ImmutableBitSet fieldsUsed, Set<AlgDataTypeField> extraFields ) {
        final AlgDataType rowType = setOp.getRowType();
        final int fieldCount = rowType.getFieldCount();
        int changeCount = 0;

        // Fennel abhors an empty row type, so pretend that the parent alg wants the last field. (The last field is the least likely to be a system field.)
        if ( fieldsUsed.isEmpty() ) {
            fieldsUsed = ImmutableBitSet.of( rowType.getFieldCount() - 1 );
        }

        // Compute the desired field mapping. Give the consumer the fields they want, in the order that they appear in the bitset.
        final Mapping mapping = createMapping( fieldsUsed, fieldCount );

        // Create input with trimmed columns.
        for ( AlgNode input : setOp.getInputs() ) {
            TrimResult trimResult = trimChild( setOp, input, fieldsUsed, extraFields );

            // We want "mapping", the input gave us "inputMapping", compute "remaining" mapping.
            //    |                   |                |
            //    |---------------- mapping ---------->|
            //    |-- inputMapping -->|                |
            //    |                   |-- remaining -->|
            //
            // For instance, suppose we have columns [a, b, c, d], the consumer asked for mapping = [b, d], and the transformed input has columns inputMapping = [d, a, b].
            // remaining will permute [b, d] to [d, a, b].
            Mapping remaining = Mappings.divide( mapping, trimResult.right );

            // Create a projection; does nothing if remaining is identity.
            algBuilder.push( trimResult.left );
            algBuilder.permute( remaining );

            if ( input != algBuilder.peek() ) {
                ++changeCount;
            }
        }

        // If the input is unchanged, and we need to project all columns, there's to do.
        if ( changeCount == 0 && mapping.isIdentity() ) {
            for ( AlgNode input : setOp.getInputs() ) {
                algBuilder.build();
            }
            return result( setOp, mapping );
        }

        switch ( setOp.kind ) {
            case UNION:
                algBuilder.union( setOp.all, setOp.getInputs().size() );
                break;
            case INTERSECT:
                algBuilder.intersect( setOp.all, setOp.getInputs().size() );
                break;
            case EXCEPT:
                assert setOp.getInputs().size() == 2;
                algBuilder.minus( setOp.all );
                break;
            default:
                throw new AssertionError( "unknown setOp " + setOp );
        }
        return result( algBuilder.build(), mapping );
    }


    /**
     * Variant of {@link #trimFields(AlgNode, ImmutableBitSet, Set)} for {@link LogicalAggregate}.
     */
    public TrimResult trimFields( Aggregate aggregate, ImmutableBitSet fieldsUsed, Set<AlgDataTypeField> extraFields ) {
        // Fields:
        //
        // | sys fields | group fields | indicator fields | agg functions |
        //
        // Two kinds of trimming:
        //
        // 1. If agg alg has system fields but none of these are used, create an agg alg with no system fields.
        //
        // 2. If aggregate functions are not used, remove them.
        //
        // But group and indicator fields stay, even if they are not used.

        final AlgDataType rowType = aggregate.getRowType();

        // Compute which input fields are used.
        // 1. group fields are always used
        final ImmutableBitSet.Builder inputFieldsUsed = aggregate.getGroupSet().rebuild();
        // 2. agg functions
        for ( AggregateCall aggCall : aggregate.getAggCallList() ) {
            inputFieldsUsed.addAll( aggCall.getArgList() );
            if ( aggCall.filterArg >= 0 ) {
                inputFieldsUsed.set( aggCall.filterArg );
            }
            inputFieldsUsed.addAll( AlgCollations.ordinals( aggCall.collation ) );
        }

        // Create input with trimmed columns.
        final AlgNode input = aggregate.getInput();
        final Set<AlgDataTypeField> inputExtraFields = Collections.emptySet();
        final TrimResult trimResult = trimChild( aggregate, input, inputFieldsUsed.build(), inputExtraFields );
        final AlgNode newInput = trimResult.left;
        final Mapping inputMapping = trimResult.right;

        // We have to return group keys and (if present) indicators. So, pretend that the consumer asked for them.
        final int groupCount = aggregate.getGroupSet().cardinality();
        final int indicatorCount = aggregate.getIndicatorCount();
        fieldsUsed = fieldsUsed.union( ImmutableBitSet.range( groupCount + indicatorCount ) );

        // If the input is unchanged, and we need to project all columns, there's nothing to do.
        if ( input == newInput && fieldsUsed.equals( ImmutableBitSet.range( rowType.getFieldCount() ) ) ) {
            return result( aggregate, Mappings.createIdentity( rowType.getFieldCount() ) );
        }

        // Which agg calls are used by our consumer?
        int j = groupCount + indicatorCount;
        int usedAggCallCount = 0;
        for ( int i = 0; i < aggregate.getAggCallList().size(); i++ ) {
            if ( fieldsUsed.get( j++ ) ) {
                ++usedAggCallCount;
            }
        }

        // Offset due to the number of system fields having changed.
        Mapping mapping =
                Mappings.create(
                        MappingType.INVERSE_SURJECTION,
                        rowType.getFieldCount(),
                        groupCount + indicatorCount + usedAggCallCount );

        final ImmutableBitSet newGroupSet = Mappings.apply( inputMapping, aggregate.getGroupSet() );

        final ImmutableList<ImmutableBitSet> newGroupSets =
                ImmutableList.copyOf(
                        Iterables.transform(
                                aggregate.getGroupSets(),
                                input1 -> Mappings.apply( inputMapping, input1 ) ) );

        // Populate mapping of where to find the fields. System, group key and indicator fields first.
        for ( j = 0; j < groupCount + indicatorCount; j++ ) {
            mapping.set( j, j );
        }

        // Now create new agg calls, and populate mapping for them.
        algBuilder.push( newInput );
        final List<AlgBuilder.AggCall> newAggCallList = new ArrayList<>();
        j = groupCount + indicatorCount;
        for ( AggregateCall aggCall : aggregate.getAggCallList() ) {
            if ( fieldsUsed.get( j ) ) {
                final ImmutableList<RexNode> args = algBuilder.fields( Mappings.apply2( inputMapping, aggCall.getArgList() ) );
                final RexNode filterArg = aggCall.filterArg < 0
                        ? null
                        : algBuilder.field( Mappings.apply( inputMapping, aggCall.filterArg ) );
                AlgBuilder.AggCall newAggCall =
                        algBuilder.aggregateCall( aggCall.getAggregation(), args )
                                .distinct( aggCall.isDistinct() )
                                .filter( filterArg )
                                .approximate( aggCall.isApproximate() )
                                .sort( algBuilder.fields( aggCall.collation ) )
                                .as( aggCall.name );
                mapping.set( j, groupCount + indicatorCount + newAggCallList.size() );
                newAggCallList.add( newAggCall );
            }
            ++j;
        }

        final AlgBuilder.GroupKey groupKey = algBuilder.groupKey( newGroupSet, newGroupSets );
        algBuilder.aggregate( groupKey, newAggCallList );

        return result( algBuilder.build(), mapping );
    }


    /**
     * Variant of {@link #trimFields(AlgNode, ImmutableBitSet, Set)} for {@link LogicalModify}.
     */
    public TrimResult trimFields( LogicalModify modifier, ImmutableBitSet fieldsUsed, Set<AlgDataTypeField> extraFields ) {
        // Ignore what consumer wants. We always project all columns.
        Util.discard( fieldsUsed );

        final AlgDataType rowType = modifier.getRowType();
        final int fieldCount = rowType.getFieldCount();
        AlgNode input = modifier.getInput();

        // We want all fields from the child.
        final int inputFieldCount = input.getRowType().getFieldCount();
        final ImmutableBitSet inputFieldsUsed = ImmutableBitSet.range( inputFieldCount );

        // Create input with trimmed columns.
        final Set<AlgDataTypeField> inputExtraFields = Collections.emptySet();
        TrimResult trimResult = trimChild( modifier, input, inputFieldsUsed, inputExtraFields );
        AlgNode newInput = trimResult.left;
        final Mapping inputMapping = trimResult.right;
        if ( !inputMapping.isIdentity() ) {
            // We asked for all fields. Can't believe that the child decided to permute them!
            throw new AssertionError( "Expected identity mapping, got " + inputMapping );
        }

        LogicalModify newModifier = modifier;
        if ( newInput != input ) {
            newModifier =
                    modifier.copy(
                            modifier.getTraitSet(),
                            Collections.singletonList( newInput ) );
        }
        assert newModifier.getClass() == modifier.getClass();

        // Always project all fields.
        Mapping mapping = Mappings.createIdentity( fieldCount );
        return result( newModifier, mapping );
    }


    /**
     * Variant of {@link #trimFields(AlgNode, ImmutableBitSet, Set)} for {@link LogicalTableFunctionScan}.
     */
    public TrimResult trimFields( LogicalTableFunctionScan tabFun, ImmutableBitSet fieldsUsed, Set<AlgDataTypeField> extraFields ) {
        final AlgDataType rowType = tabFun.getRowType();
        final int fieldCount = rowType.getFieldCount();
        final List<AlgNode> newInputs = new ArrayList<>();

        for ( AlgNode input : tabFun.getInputs() ) {
            final int inputFieldCount = input.getRowType().getFieldCount();
            ImmutableBitSet inputFieldsUsed = ImmutableBitSet.range( inputFieldCount );

            // Create input with trimmed columns.
            final Set<AlgDataTypeField> inputExtraFields = Collections.emptySet();
            TrimResult trimResult = trimChildRestore( tabFun, input, inputFieldsUsed, inputExtraFields );
            assert trimResult.right.isIdentity();
            newInputs.add( trimResult.left );
        }

        LogicalTableFunctionScan newTabFun = tabFun;
        if ( !tabFun.getInputs().equals( newInputs ) ) {
            newTabFun = tabFun.copy(
                    tabFun.getTraitSet(),
                    newInputs,
                    tabFun.getCall(),
                    tabFun.getElementType(),
                    tabFun.getRowType(),
                    tabFun.getColumnMappings() );
        }
        assert newTabFun.getClass() == tabFun.getClass();

        // Always project all fields.
        Mapping mapping = Mappings.createIdentity( fieldCount );
        return result( newTabFun, mapping );
    }


    /**
     * Variant of {@link #trimFields(AlgNode, ImmutableBitSet, Set)} for {@link LogicalValues}.
     */
    public TrimResult trimFields( LogicalValues values, ImmutableBitSet fieldsUsed, Set<AlgDataTypeField> extraFields ) {
        final AlgDataType rowType = values.getRowType();
        final int fieldCount = rowType.getFieldCount();

        // If they are asking for no fields, we can't give them what they want, because zero-column records are illegal. Give them the last field, which is unlikely to be a system field.
        if ( fieldsUsed.isEmpty() ) {
            fieldsUsed = ImmutableBitSet.range( fieldCount - 1, fieldCount );
        }

        // If all fields are used, return unchanged.
        if ( fieldsUsed.equals( ImmutableBitSet.range( fieldCount ) ) ) {
            Mapping mapping = Mappings.createIdentity( fieldCount );
            return result( values, mapping );
        }

        final ImmutableList.Builder<ImmutableList<RexLiteral>> newTuples = ImmutableList.builder();
        for ( ImmutableList<RexLiteral> tuple : values.getTuples() ) {
            ImmutableList.Builder<RexLiteral> newTuple = ImmutableList.builder();
            for ( int field : fieldsUsed ) {
                newTuple.add( tuple.get( field ) );
            }
            newTuples.add( newTuple.build() );
        }

        final Mapping mapping = createMapping( fieldsUsed, fieldCount );
        final AlgDataType newRowType = AlgOptUtil.permute( values.getCluster().getTypeFactory(), rowType, mapping );
        final LogicalValues newValues = LogicalValues.create( values.getCluster(), newRowType, newTuples.build() );
        return result( newValues, mapping );
    }


    protected Mapping createMapping( ImmutableBitSet fieldsUsed, int fieldCount ) {
        final Mapping mapping =
                Mappings.create(
                        MappingType.INVERSE_SURJECTION,
                        fieldCount,
                        fieldsUsed.cardinality() );
        int i = 0;
        for ( int field : fieldsUsed ) {
            mapping.set( field, i++ );
        }
        return mapping;
    }


    /**
     * Variant of {@link #trimFields(AlgNode, ImmutableBitSet, Set)} for {@link LogicalScan}.
     */
    public TrimResult trimFields( final Scan tableAccessRel, ImmutableBitSet fieldsUsed, Set<AlgDataTypeField> extraFields ) {
        final int fieldCount = tableAccessRel.getRowType().getFieldCount();
        if ( fieldsUsed.equals( ImmutableBitSet.range( fieldCount ) ) && extraFields.isEmpty() ) {
            // If there is nothing to project or if we are projecting everything then no need to introduce another AlgNode
            return trimFields( (AlgNode) tableAccessRel, fieldsUsed, extraFields );
        }
        final AlgNode newTableAccessAlg = tableAccessRel.project( fieldsUsed, extraFields, algBuilder );

        // Some parts of the system can't handle rows with zero fields, so pretend that one field is used.
        if ( fieldsUsed.cardinality() == 0 ) {
            AlgNode input = newTableAccessAlg;
            if ( input instanceof Project ) {
                // The table has implemented the project in the obvious way - by creating project with 0 fields. Strip it away, and create our own project with one field.
                Project project = (Project) input;
                if ( project.getRowType().getFieldCount() == 0 ) {
                    input = project.getInput();
                }
            }
            return dummyProject( fieldCount, input );
        }

        final Mapping mapping = createMapping( fieldsUsed, fieldCount );
        return result( newTableAccessAlg, mapping );
    }


    /**
     * Result of an attempt to trim columns from a relational expression.
     *
     * The mapping describes where to find the columns wanted by the parent of the current relational expression.
     *
     * The mapping is a {@link org.polypheny.db.util.mapping.Mappings.SourceMapping}, which means that no column can be used more than once, and some columns are not used.
     * {@code columnsUsed.getSource(i)} returns the source of the i'th output field.
     *
     * For example, consider the mapping for a relational expression that has 4 output columns but only two are being used. The mapping {2 &rarr; 1, 3 &rarr; 0} would give the following behavior:
     *
     * <ul>
     * <li>columnsUsed.getSourceCount() returns 4
     * <li>columnsUsed.getTargetCount() returns 2
     * <li>columnsUsed.getSource(0) returns 3
     * <li>columnsUsed.getSource(1) returns 2
     * <li>columnsUsed.getSource(2) throws IndexOutOfBounds
     * <li>columnsUsed.getTargetOpt(3) returns 0
     * <li>columnsUsed.getTargetOpt(0) returns -1
     * </ul>
     */
    protected static class TrimResult extends Pair<AlgNode, Mapping> {

        /**
         * Creates a TrimResult.
         *
         * @param left New relational expression
         * @param right Mapping of fields onto original fields
         */
        public TrimResult( AlgNode left, Mapping right ) {
            super( left, right );
            assert right.getTargetCount() == left.getRowType().getFieldCount() : "rowType: " + left.getRowType() + ", mapping: " + right;
        }

    }

}


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

package org.polypheny.db.adapter.cassandra;


import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.ColumnRelationBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.polypheny.db.adapter.cassandra.rules.CassandraRules;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;


/**
 * Implementation of a {@link Filter}
 * relational expression in Cassandra.
 */
public class CassandraFilter extends Filter implements CassandraAlg {

    private Boolean singlePartition;
    private AlgCollation implicitCollation;


    public CassandraFilter(
            AlgOptCluster cluster,
            AlgTraitSet traitSet,
            AlgNode child,
            RexNode condition,
            List<String> partitionKeys,
            List<String> clusteringKeys,
            List<AlgFieldCollation> implicitFieldCollations ) {
        super( cluster, traitSet, child, condition );

        this.singlePartition = false;
//        List<String> clusteringKeys1 = new ArrayList<>( clusteringKeys );

//        Translator translator = new Translator( getRowType(), partitionKeys, clusteringKeys, implicitFieldCollations );
//        this.match = translator.translateMatch( condition );
        // Testing if this is really needed...
//        this.singlePartition = translator.isSinglePartition();
//        this.implicitCollation = translator.getImplicitCollation();

        // TODO JS: Check this
//        assert getConvention() == CONVENTION;
//        assert getConvention() == child.getConvention();
    }


    public CassandraFilter( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode convert, RexNode condition ) {
        this( cluster, traitSet, convert, condition, new ArrayList<>(), new ArrayList<>(), new ArrayList<>() );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( CassandraConvention.COST_MULTIPLIER );
    }


    @Override
    public CassandraFilter copy( AlgTraitSet traitSet, AlgNode input, RexNode condition ) {
        return new CassandraFilter( getCluster(), traitSet, input, condition );
    }


    @Override
    public void implement( CassandraImplementContext context ) {
        context.visitChild( 0, getInput() );

        context.filterCollation = this.getImplicitCollation();

        final Pair<List<String>, List<String>> keyFields = context.cassandraTable.getPhysicalKeyFields();

        Translator translator = new Translator( context.cassandraTable.getRowType( new JavaTypeFactoryImpl() ), keyFields.left, keyFields.right, context.cassandraTable.getClusteringOrder() );

        List<Relation> match = translator.translateMatch( condition );

        context.addWhereRelations( match );
    }


    /**
     * Check if the filter restricts to a single partition.
     *
     * @return True if the filter will restrict the underlying to a single partition
     */
    public boolean isSinglePartition() {
        return singlePartition;
    }


    /**
     * Get the resulting collation by the clustering keys after filtering.
     *
     * @return The implicit collation based on the natural sorting by clustering keys
     */
    public AlgCollation getImplicitCollation() {
        return implicitCollation;
    }


    /**
     * Translates {@link RexNode} expressions into Cassandra expression strings.
     */
    static class Translator {

        private final AlgDataType rowType;
        private final List<String> fieldNames;
        private final Set<String> partitionKeys;
        private final List<String> clusteringKeys;
        private int restrictedClusteringKeys;
        private final List<AlgFieldCollation> implicitFieldCollations;


        Translator( AlgDataType rowType, List<String> partitionKeys, List<String> clusteringKeys, List<AlgFieldCollation> implicitFieldCollations ) {
            this.rowType = rowType;
            this.fieldNames = CassandraRules.cassandraPhysicalFieldNames( rowType );
            this.partitionKeys = new HashSet<>( partitionKeys );
            this.clusteringKeys = clusteringKeys;
            this.restrictedClusteringKeys = 0;
            this.implicitFieldCollations = implicitFieldCollations;
        }


        /**
         * Check if the query spans only one partition.
         *
         * @return True if the matches translated so far have resulted in a single partition
         */
        public boolean isSinglePartition() {
            return partitionKeys.isEmpty();
        }


        /**
         * Infer the implicit correlation from the unrestricted clustering keys.
         *
         * @return The collation of the filtered results
         */
        public AlgCollation getImplicitCollation() {
            // No collation applies if we aren't restricted to a single partition
            if ( !isSinglePartition() ) {
                return AlgCollations.EMPTY;
            }

            // Pull out the correct fields along with their original collations
            List<AlgFieldCollation> fieldCollations = new ArrayList<>();
            for ( int i = restrictedClusteringKeys; i < clusteringKeys.size(); i++ ) {
                int fieldIndex = fieldNames.indexOf( clusteringKeys.get( i ) );
                AlgFieldCollation.Direction direction = implicitFieldCollations.get( i ).getDirection();
                fieldCollations.add( new AlgFieldCollation( fieldIndex, direction ) );
            }

            return AlgCollations.of( fieldCollations );
        }


        /**
         * Produce the CQL predicate string for the given condition.
         *
         * @param condition Condition to translate
         * @return CQL predicate string
         */
        private List<Relation> translateMatch( RexNode condition ) {
            // CQL does not support disjunctions
            List<RexNode> disjunctions = AlgOptUtil.disjunctions( condition );
            if ( disjunctions.size() == 1 ) {
                return translateAnd( disjunctions.get( 0 ) );
            } else {
                throw new AssertionError( "cannot translate " + condition );
            }
        }


        /**
         * Convert the value of a literal to a string.
         *
         * @param literal Literal to translate
         * @return String representation of the literal
         */
        private static Object literalValue( RexLiteral literal ) {
            Object value = CassandraValues.literalValue( literal );
            return value;
        }


        /**
         * Translate a conjunctive predicate to a CQL string.
         *
         * @param condition A conjunctive predicate
         * @return CQL string for the predicate
         */
        private List<Relation> translateAnd( RexNode condition ) {
            List<Relation> predicates = new ArrayList<>();
            for ( RexNode node : AlgOptUtil.conjunctions( condition ) ) {
                predicates.add( translateMatch2( node ) );
            }

            return predicates;
        }


        /**
         * Translate a binary relation.
         */
        private Relation translateMatch2( RexNode node ) {
            // We currently only use equality, but inequalities on clustering keys should be possible in the future
            switch ( node.getKind() ) {
                case EQUALS:
                    return translateBinary( Kind.EQUALS, Kind.EQUALS, (RexCall) node );
                case LESS_THAN:
                    return translateBinary( Kind.LESS_THAN, Kind.GREATER_THAN, (RexCall) node );
                case LESS_THAN_OR_EQUAL:
                    return translateBinary( Kind.LESS_THAN_OR_EQUAL, Kind.GREATER_THAN_OR_EQUAL, (RexCall) node );
                case GREATER_THAN:
                    return translateBinary( Kind.GREATER_THAN, Kind.LESS_THAN, (RexCall) node );
                case GREATER_THAN_OR_EQUAL:
                    return translateBinary( Kind.GREATER_THAN_OR_EQUAL, Kind.LESS_THAN_OR_EQUAL, (RexCall) node );
                default:
                    throw new AssertionError( "cannot translate " + node );
            }
        }


        /**
         * Translates a call to a binary operator, reversing arguments if necessary.
         */
        private Relation translateBinary( Kind op, Kind rop, RexCall call ) {
            final RexNode left = call.operands.get( 0 );
            final RexNode right = call.operands.get( 1 );
            Relation expression = translateBinary2( op, left, right );
            if ( expression != null ) {
                return expression;
            }
            expression = translateBinary2( rop, right, left );
            if ( expression != null ) {
                return expression;
            }
            throw new AssertionError( "cannot translate op " + op + " call " + call );
        }


        /**
         * Translates a call to a binary operator. Returns null on failure.
         */
        private Relation translateBinary2( Kind op, RexNode left, RexNode right ) {
            switch ( right.getKind() ) {
                case LITERAL:
                    break;
                default:
                    return null;
            }
            final RexLiteral rightLiteral = (RexLiteral) right;
            switch ( left.getKind() ) {
                case INPUT_REF:
                    final RexInputRef left1 = (RexInputRef) left;
                    String name = fieldNames.get( left1.getIndex() );
                    return translateOp2( op, name, rightLiteral );
                case CAST:
                    // FIXME This will not work in all cases (for example, we ignore string encoding)
                    return translateBinary2( op, ((RexCall) left).operands.get( 0 ), right );
                default:
                    return null;
            }
        }


        /**
         * Combines a field name, operator, and literal to produce a predicate string.
         */
        private Relation translateOp2( Kind op, String name, RexLiteral right ) {
            // In case this is a key, record that it is now restricted
            if ( op.equals( "=" ) ) {
                partitionKeys.remove( name );
                if ( clusteringKeys.contains( name ) ) {
                    restrictedClusteringKeys++;
                }
            }

            Object value = literalValue( right );
            String valueString = value.toString();
            if ( value instanceof String ) {
                PolyType typeName = rowType.getField( name, true, false ).getType().getPolyType();
                if ( typeName != PolyType.CHAR ) {
                    valueString = "'" + valueString + "'";
                }
            }

            ColumnRelationBuilder<Relation> alg = Relation.column( name );
            Term term = QueryBuilder.literal( value );
            switch ( op ) {
                case EQUALS:
                    return alg.isEqualTo( term );
                case LESS_THAN:
                    return alg.isLessThan( term );
                case LESS_THAN_OR_EQUAL:
                    return alg.isLessThanOrEqualTo( term );
                case GREATER_THAN:
                    return alg.isGreaterThan( term );
                case GREATER_THAN_OR_EQUAL:
                    return alg.isLessThanOrEqualTo( term );
                default:
                    throw new AssertionError( "cannot translate op " + op + " name " + name + " valuestring " + valueString );
            }
        }

    }

}


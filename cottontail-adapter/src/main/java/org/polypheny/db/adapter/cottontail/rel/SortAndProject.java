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

package org.polypheny.db.adapter.cottontail.rel;


import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelFieldCollation;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.SingleRel;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.Pair;


public abstract class SortAndProject extends SingleRel {


    // Sort part
    public final RelCollation collation;
    protected final ImmutableList<RexNode> fieldExps;
    public final RexNode offset;
    public final RexNode fetch;

    // Project part
    protected final ImmutableList<RexNode> exps;
    protected final RelDataType projectRowType;
    protected final boolean arrayProject;


    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param child Input relational expression
     */
    protected SortAndProject( RelOptCluster cluster, RelTraitSet traits, RelNode child, RelCollation collation, RexNode offset, RexNode fetch, List<? extends RexNode> projects, RelDataType rowType, RelDataType projectRowType, boolean arrayProject ) {
        super( cluster, traits, child );

        // Sort part
        this.collation = collation;
        this.offset = offset;
        this.fetch = fetch;

        assert traits.containsIfApplicable( collation ) : "traits=" + traits + ", collation=" + collation;
        assert !(fetch == null && offset == null && collation.getFieldCollations().isEmpty()) : "trivial sort";
        ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
        for ( RelFieldCollation field : collation.getFieldCollations() ) {
            int index = field.getFieldIndex();
            builder.add( cluster.getRexBuilder().makeInputRef( child, index ) );
        }
        fieldExps = builder.build();

        // Project part
        this.exps = ImmutableList.copyOf( projects );
        this.rowType = rowType;
        this.projectRowType = projectRowType;
        this.arrayProject = arrayProject;

    }


    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return copy( traitSet, sole( inputs ), exps, collation, offset, fetch );
    }


    public abstract SortAndProject copy( RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelCollation newCollation, RexNode offset, RexNode fetch );


    /**
     * Returns the project expressions.
     *
     * @return Project expressions
     */
    public List<RexNode> getProjects() {
        return exps;
    }


    /**
     * Returns a list of (expression, name) pairs. Convenient for various transformations.
     *
     * @return List of (expression, name) pairs
     */
    public final List<Pair<RexNode, String>> getNamedProjects() {
        return Pair.zip( getProjects(), getRowType().getFieldNames() );
    }


    /**
     * Returns the array of {@link RelFieldCollation}s asked for by the sort specification, from most significant to least significant.
     *
     * See also {@link RelMetadataQuery#collations(RelNode)}, which lists all known collations. For example,
     * <code>ORDER BY time_id</code> might also be sorted by
     * <code>the_year, the_month</code> because of a known monotonicity constraint among the columns. {@code getCollation} would return
     * <code>[time_id]</code> and {@code collations} would return
     * <code>[ [time_id], [the_year, the_month] ]</code>.
     */
    public RelCollation getCollation() {
        return collation;
    }


    @Override
    public List<RexNode> getChildExps() {
        return exps;
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        double dRows = mq.getRowCount( getInput() );
        double dCpu = dRows * exps.size();
        double dIo = 0;
        return planner.getCostFactory().makeCost( dRows, dCpu, dIo );
    }


    @Override
    public String relCompareString() {
        return this.getClass().getSimpleName() + "$" +
                input.relCompareString() + "$" +
                (exps != null ? exps.stream().map( Objects::hashCode ).map( Objects::toString ).collect( Collectors.joining( "$" ) ) : "") + "$" +
                rowType.toString() + "$" +
                (collation != null ? collation.getFieldCollations().stream().map( RelFieldCollation::getDirection ).map( Objects::toString ).collect( Collectors.joining( "$" ) ) : "") + "$" +
                (offset != null ? offset.hashCode() : "") + "$" +
                (fetch != null ? fetch.hashCode() : "") + "&";
    }

}

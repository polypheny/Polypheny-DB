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

package org.polypheny.db.cypher.clause;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgFieldCollation.Direction;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.graph.LogicalGraphProject;
import org.polypheny.db.algebra.logical.graph.LogicalGraphSort;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.Pair;

@Getter
public class CypherReturnClause extends CypherClause {

    private final boolean distinct;
    private final List<CypherReturn> returnItems;
    private final List<CypherOrderItem> order;
    private final ParserPos pos1;
    private final CypherExpression skip;
    private final ParserPos pos2;
    private final CypherExpression limit;
    private final ParserPos pos3;


    public CypherReturnClause( ParserPos pos, boolean distinct, List<CypherReturn> returnItems, List<CypherOrderItem> order, ParserPos pos1, CypherExpression skip, ParserPos pos2, CypherExpression limit, ParserPos pos3 ) {
        super( pos );
        this.distinct = distinct;
        this.returnItems = returnItems;
        this.order = order;
        this.pos1 = pos1;
        this.skip = skip;
        this.pos2 = pos2;
        this.limit = limit;
        this.pos3 = pos3;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.RETURN;
    }


    public AlgNode getGraphProject( CypherContext context ) {
        List<Pair<String, RexNode>> nameAndProject = returnItems.stream().map( i -> i.getRexAsProject( context ) ).collect( Collectors.toList() );
        AlgNode node = context.pop();

        AlgNode project = new LogicalGraphProject(
                context.cluster,
                context.cluster.traitSet(),
                node,
                Pair.right( nameAndProject ),
                Pair.left( nameAndProject )
        );

        if ( node.getRowType().equals( project.getRowType() ) ) {
            return node;
        }
        return project;

    }


    public AlgNode getGraphSort( CypherContext context ) {
        List<Pair<Direction, String>> orders = order.stream().map( o ->
                Pair.of( o.isAsc() ? Direction.ASCENDING : Direction.DESCENDING, o.getExpression().getName() ) ).collect( Collectors.toList() );

        AlgNode node = context.peek();

        List<AlgFieldCollation> collations = new ArrayList<>();

        for ( Pair<Direction, String> item : orders ) {
            int index = node.getRowType().getFieldNames().indexOf( item.right );

            collations.add( new AlgFieldCollation( index, item.left ) );
        }

        AlgCollation collation = AlgCollations.of( collations );

        Integer skip = null;
        if ( this.skip != null ) {
            skip = (Integer) this.skip.getComparable();
        }

        Integer limit = null;
        if ( this.limit != null ) {
            limit = (Integer) this.limit.getComparable();
        }

        AlgTraitSet traitSet = node.getTraitSet().replace( collation );

        return new LogicalGraphSort( node.getCluster(), traitSet, collation, context.pop(), skip, limit );
    }


    public List<String> getSortFields() {
        return order.stream().map( o -> o.getExpression().getName() ).collect( Collectors.toList() );
    }

}

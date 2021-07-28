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

package org.polypheny.db.router;

import java.util.HashSet;
import lombok.Getter;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelShuttleImpl;
import org.polypheny.db.rel.core.TableFunctionScan;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.rel.logical.LogicalAggregate;
import org.polypheny.db.rel.logical.LogicalCorrelate;
import org.polypheny.db.rel.logical.LogicalExchange;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalIntersect;
import org.polypheny.db.rel.logical.LogicalJoin;
import org.polypheny.db.rel.logical.LogicalMatch;
import org.polypheny.db.rel.logical.LogicalMinus;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.logical.LogicalSort;
import org.polypheny.db.rel.logical.LogicalUnion;
import org.polypheny.db.rel.logical.LogicalValues;

// TODO MV: This should be improved to include more information on the used tables and columns
public class QueryNameShuttle extends RelShuttleImpl {
    @Getter
    protected final HashSet<String> hashBasis = new HashSet<>();


    @Override
    public RelNode visit( LogicalAggregate aggregate ) {
        hashBasis.add( "LogicalAggregate#" + aggregate.getAggCallList() );
        return visitChild( aggregate, 0, aggregate.getInput() );
    }


    @Override
    public RelNode visit( LogicalMatch match ) {
        hashBasis.add( "LogicalMatch#" + match.getTable().getQualifiedName() );
        return visitChild( match, 0, match.getInput() );
    }


    @Override
    public RelNode visit( TableScan scan ) {
        hashBasis.add( "TableScan#" + scan.getTable().getQualifiedName() );
        return scan;
    }


    @Override
    public RelNode visit( TableFunctionScan scan ) {
        hashBasis.add( "TableFunctionScan#" + scan.getTable().getQualifiedName() ); // TODO: This is most probably not sufficient
        return visitChildren( scan );
    }


    @Override
    public RelNode visit( LogicalValues values ) {
        return values;
    }


    @Override
    public RelNode visit( LogicalFilter filter ) {
        hashBasis.add( "LogicalFilter" );
        return visitChild( filter, 0, filter.getInput() );
    }


    @Override
    public RelNode visit( LogicalProject project ) {
        hashBasis.add( "LogicalProject#" + project.getProjects().size() );
        return visitChild( project, 0, project.getInput() );
    }


    @Override
    public RelNode visit( LogicalJoin join ) {
        hashBasis.add( "LogicalJoin#" + join.getLeft().getTable().getQualifiedName() + "#" + join.getRight().getTable().getQualifiedName() );
        return visitChildren( join );
    }


    @Override
    public RelNode visit( LogicalCorrelate correlate ) {
        hashBasis.add( "LogicalCorrelate" );
        return visitChildren( correlate );
    }


    @Override
    public RelNode visit( LogicalUnion union ) {
        hashBasis.add( "LogicalUnion" );
        return visitChildren( union );
    }


    @Override
    public RelNode visit( LogicalIntersect intersect ) {
        hashBasis.add( "LogicalIntersect" );
        return visitChildren( intersect );
    }


    @Override
    public RelNode visit( LogicalMinus minus ) {
        hashBasis.add( "LogicalMinus" );
        return visitChildren( minus );
    }


    @Override
    public RelNode visit( LogicalSort sort ) {
        hashBasis.add( "LogicalSort" );
        return visitChildren( sort );
    }


    @Override
    public RelNode visit( LogicalExchange exchange ) {
        hashBasis.add( "LogicalExchange#" + exchange.distribution.getType().shortName );
        return visitChildren( exchange );
    }


    @Override
    public RelNode visit( RelNode other ) {
        hashBasis.add( "other#" + other.getClass().getSimpleName() );
        return visitChildren( other );
    }
}

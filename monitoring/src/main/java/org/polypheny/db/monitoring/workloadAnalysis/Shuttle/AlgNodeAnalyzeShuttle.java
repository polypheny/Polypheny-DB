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

package org.polypheny.db.monitoring.workloadAnalysis.Shuttle;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.TableScan;
import org.polypheny.db.algebra.logical.LogicalAggregate;
import org.polypheny.db.algebra.logical.LogicalCorrelate;
import org.polypheny.db.algebra.logical.LogicalExchange;
import org.polypheny.db.algebra.logical.LogicalFilter;
import org.polypheny.db.algebra.logical.LogicalIntersect;
import org.polypheny.db.algebra.logical.LogicalJoin;
import org.polypheny.db.algebra.logical.LogicalMatch;
import org.polypheny.db.algebra.logical.LogicalMinus;
import org.polypheny.db.algebra.logical.LogicalProject;
import org.polypheny.db.algebra.logical.LogicalSort;
import org.polypheny.db.algebra.logical.LogicalTableModify;
import org.polypheny.db.algebra.logical.LogicalTableScan;
import org.polypheny.db.algebra.logical.LogicalUnion;
import org.polypheny.db.monitoring.workloadAnalysis.InformationObjects.AggregateInformation;
import org.polypheny.db.monitoring.workloadAnalysis.InformationObjects.JoinInformation;
import org.polypheny.db.monitoring.workloadAnalysis.InformationObjects.TableScanInformation;
import org.polypheny.db.schema.LogicalTable;

@Slf4j
@Getter
public class AlgNodeAnalyzeShuttle extends AlgShuttleImpl {

    protected final AlgNodeAnalyzeRexShuttle rexShuttle;

    private final AggregateInformation aggregateInformation = new AggregateInformation();
    private JoinInformation joinInformation = new JoinInformation(  );
    private final TableScanInformation tableScanInformation = new TableScanInformation(  );
    private int projectCount;
    private int sortCount;
    private int filterCount;


    public AlgNodeAnalyzeShuttle() {
        this.rexShuttle = new AlgNodeAnalyzeRexShuttle();
    }


    @Override
    public AlgNode visit( LogicalAggregate aggregate ) {
        log.warn( "LogicalAggregate#" + aggregate.getAggCallList() );

        for ( AggregateCall test : aggregate.getAggCallList() ) {
            aggregateInformation.incrementAggregateInformation(
                    test.getAggregation().getKind() );
        }

        return visitChild( aggregate, 0, aggregate.getInput() );
    }


    @Override
    public AlgNode visit( LogicalMatch match ) {
        log.warn( "LogicalMatch#" + match.getTable().getQualifiedName() );
        return visitChild( match, 0, match.getInput() );
    }


    @Override
    public AlgNode visit( TableScan scan ) {
        log.warn( "TableScan#" + scan.getTable().getQualifiedName() );
        tableScanInformation.updateTableScanInfo( scan.getTable().getTable().getTableId() );
        return super.visit( scan );
    }


    @Override
    public AlgNode visit( LogicalFilter filter ) {
        log.warn( "LogicalFilter" );
        this.filterCount += 1;
        super.visit( filter );
        filter.accept( this.rexShuttle );

        return filter;
    }


    @Override
    public AlgNode visit( LogicalProject project ) {
        log.warn( "LogicalProject#" + project.getProjects().size() );
        this.projectCount += 1;
        super.visit( project );
        project.accept( this.rexShuttle );
        return project;
    }


    @Override
    public AlgNode visit( LogicalCorrelate correlate ) {
        log.warn( "LogicalCorrelate" );
        return visitChildren( correlate );
    }


    @Override
    public AlgNode visit( LogicalJoin join ) {
        if ( join.getLeft() instanceof LogicalTableScan && join.getRight() instanceof LogicalTableScan ) {
            log.warn( "LogicalJoin#" + join.getLeft().getTable().getQualifiedName() + "#" + join.getRight().getTable().getQualifiedName() );
            joinInformation.updateJoinInformation(join.getLeft().getTable().getTable().getTableId(),join.getRight().getTable().getTable().getTableId() );
        }

        super.visit( join );
        join.accept( this.rexShuttle );
        return join;
    }


    @Override
    public AlgNode visit( LogicalUnion union ) {
        log.warn( "LogicalUnion" );
        super.visit( union );
        union.accept( this.rexShuttle );
        return union;
    }


    @Override
    public AlgNode visit( LogicalIntersect intersect ) {
        log.warn( "LogicalIntersect" );
        super.visit( intersect );
        intersect.accept( this.rexShuttle );
        return intersect;
    }


    @Override
    public AlgNode visit( LogicalMinus minus ) {
        log.warn( "LogicalMinus" );
        return visitChildren( minus );
    }


    @Override
    public AlgNode visit( LogicalSort sort ) {
        log.warn( "LogicalSort" );
        this.sortCount += 1;
        return visitChildren( sort );
    }


    @Override
    public AlgNode visit( LogicalExchange exchange ) {
        log.warn( "LogicalExchange#" + exchange.distribution.getType().shortName );
        return visitChildren( exchange );
    }


    @Override
    public AlgNode visit( AlgNode other ) {
        log.warn( "other#" + other.getClass().getSimpleName() );
        if ( other instanceof LogicalTableModify ) {
            // Add all columns to availableColumnsWithTable for statistics
            if ( (other.getTable().getTable() instanceof LogicalTable) ) {
                LogicalTable logicalTable = ((LogicalTable) other.getTable().getTable());
                Long tableId = logicalTable.getTableId();
                //logicalTable.getColumnIds().forEach( v -> availableColumnsWithTable.put( v, tableId ) );
            }
        }
        return visitChildren( other );
    }


}

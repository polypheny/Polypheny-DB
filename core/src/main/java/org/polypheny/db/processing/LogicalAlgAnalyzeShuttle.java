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
 */

package org.polypheny.db.processing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Scan;
import org.polypheny.db.algebra.logical.common.LogicalConstraintEnforcer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentAggregate;
import org.polypheny.db.algebra.logical.document.LogicalDocumentFilter;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentSort;
import org.polypheny.db.algebra.logical.document.LogicalDocumentTransformer;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgAggregate;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgFilter;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgMatch;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgSort;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgTransformer;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgUnwind;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelCorrelate;
import org.polypheny.db.algebra.logical.relational.LogicalRelExchange;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelIntersect;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelMatch;
import org.polypheny.db.algebra.logical.relational.LogicalRelMinus;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelSort;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.transaction.Statement;


/**
 * Universal routing alg shuttle class to extract partition and column information from AlgNode.
 */
@EqualsAndHashCode(callSuper = true)
@Slf4j
@Value
public class LogicalAlgAnalyzeShuttle extends AlgShuttleImpl {

    public LogicalAlgAnalyzeRexShuttle rexShuttle;

    public Map<Long, Set<String>> partitionValueFilterPerScan = new HashMap<>(); // logical scanId (ScanId) -> (logical tableId -> List partitionsValue)

    public Set<String> hashBasis = new HashSet<>();

    public Map<Long, String> availableColumns = new LinkedHashMap<>(); // column id -> schemaName.tableName.ColumnName

    public Map<Long, Long> availableColumnsWithTable = new HashMap<>(); // columnId -> tableId

    public Map<DataModel, Set<Long>> modifiedEntities = new HashMap<>();

    public Map<DataModel, Set<Long>> scannedEntities = new HashMap<>();

    public List<Long> entityIds = new ArrayList<>();

    public Statement statement;


    public LogicalAlgAnalyzeShuttle( Statement statement ) {
        this.rexShuttle = new LogicalAlgAnalyzeRexShuttle();
        this.statement = statement;
    }


    public Map<Long, String> getUsedColumns() {
        if ( this.availableColumns.isEmpty() ) {
            return Collections.emptyMap();
        }

        final ArrayList<String> availableColumnNames = new ArrayList<>( this.availableColumns.values() );
        final ArrayList<Long> availableColumnKeys = new ArrayList<>( this.availableColumns.keySet() );

        if ( this.rexShuttle.usedIds.isEmpty() ) {
            return this.availableColumns;
        }

        Map<Long, String> result = new HashMap<>();

        for ( int usedId : this.rexShuttle.usedIds ) {

            // The number of UsedIds could be greater than number of availableColumns. This occurs if a statement contains
            // a column more than two times. E.g.col21 is present in Projection & in Filter. However, since  availableColumns
            // is a map it only stores the present ColumnIds one time. But rexShuttle.usedIds tracks every positional
            // occurrence. Therefore, this could result in more entries. We consequently need to skip those.
            if ( usedId >= availableColumnKeys.size() ) {
                continue;
            }
            result.put(
                    availableColumnKeys.get( usedId ),
                    availableColumnNames.get( usedId )
            );
        }

        return result;
    }


    public String getQueryName() {
        return this.hashBasis.toString();
    }


    private void addScannedEntity( DataModel type, long entityId ) {
        if ( !scannedEntities.containsKey( type ) ) {
            scannedEntities.put( type, new HashSet<>() );
        }
        scannedEntities.get( type ).add( entityId );
    }


    private void addModifiedEntity( DataModel type, long entityId ) {
        if ( !modifiedEntities.containsKey( type ) ) {
            modifiedEntities.put( type, new HashSet<>() );
        }
        modifiedEntities.get( type ).add( entityId );
    }


    @Override
    public AlgNode visit( LogicalRelAggregate aggregate ) {
        hashBasis.add( LogicalRelAggregate.class.getSimpleName() + "#" + aggregate.getAggCallList() );
        return visitChild( aggregate, 0, aggregate.getInput() );
    }


    @Override
    public AlgNode visit( LogicalLpgModify modify ) {
        hashBasis.add( modify.getClass().getSimpleName() );

        addModifiedEntity( modify.getEntity().dataModel, getLogicalId( modify ) );

        return super.visit( modify );
    }


    @Override
    public AlgNode visit( LogicalLpgScan scan ) {
        hashBasis.add( scan.getClass().getSimpleName() + "#" + scan.entity.id );

        addScannedEntity( scan.getEntity().dataModel, scan.entity.id );

        return super.visit( scan );
    }


    @Override
    public AlgNode visit( LogicalLpgFilter filter ) {
        hashBasis.add( filter.getClass().getSimpleName() );
        super.visit( filter );
        filter.accept( this.rexShuttle );

        //getPartitioningInfo( filter );

        return filter;
    }


    @Override
    public AlgNode visit( LogicalLpgMatch match ) {
        hashBasis.add( match.getClass().getSimpleName() );
        match.accept( this.rexShuttle );
        return visitChildren( match );
    }


    @Override
    public AlgNode visit( LogicalLpgProject project ) {
        hashBasis.add( project.getClass().getSimpleName() + "#" + project.getProjects().size() );
        super.visit( project );
        project.accept( this.rexShuttle );
        return project;
    }


    @Override
    public AlgNode visit( LogicalLpgAggregate aggregate ) {
        hashBasis.add( aggregate.getClass().getSimpleName() + "#" + aggregate.aggCalls );
        return visitChild( aggregate, 0, aggregate.getInput() );
    }


    @Override
    public AlgNode visit( LogicalLpgSort sort ) {
        hashBasis.add( sort.getClass().getSimpleName() );
        return visitChild( sort, 0, sort.getInput() );
    }


    @Override
    public AlgNode visit( LogicalLpgUnwind unwind ) {
        hashBasis.add( unwind.getClass().getSimpleName() + "#" + unwind.index + "#" + unwind.alias );
        return visitChild( unwind, 0, unwind.getInput() );
    }


    @Override
    public AlgNode visit( LogicalLpgTransformer transformer ) {
        hashBasis.add( transformer.getClass().getSimpleName() );
        return visitChildren( transformer );
    }


    @Override
    public AlgNode visit( LogicalDocumentModify modify ) {
        hashBasis.add( LogicalDocumentModify.class.getSimpleName() );

        addModifiedEntity( modify.getEntity().dataModel, getLogicalId( modify ) );

        return super.visit( modify );
    }


    @Override
    public AlgNode visit( LogicalDocumentAggregate aggregate ) {
        hashBasis.add( LogicalDocumentAggregate.class.getSimpleName() + "#" + aggregate.aggCalls );
        return visitChild( aggregate, 0, aggregate.getInput() );
    }


    @Override
    public AlgNode visit( LogicalDocumentFilter filter ) {
        hashBasis.add( LogicalDocumentFilter.class.getSimpleName() );
        super.visit( filter );
        filter.accept( this.rexShuttle );

        //getPartitioningInfo( filter );

        return filter;
    }


    @Override
    public AlgNode visit( LogicalDocumentProject project ) {
        hashBasis.add( LogicalDocumentProject.class.getSimpleName() + "#" + project.includes.size() + "$" + project.excludes.size() );
        super.visit( project );
        project.accept( this.rexShuttle );
        return project;
    }


    @Override
    public AlgNode visit( LogicalDocumentScan scan ) {
        hashBasis.add( LogicalDocumentScan.class.getSimpleName() + "#" + scan.entity.id );

        addScannedEntity( scan.entity.dataModel, getLogicalId( scan ) );

        return super.visit( scan );
    }


    @Override
    public AlgNode visit( LogicalDocumentSort sort ) {
        hashBasis.add( LogicalDocumentSort.class.getSimpleName() );
        return visitChildren( sort );
    }


    @Override
    public AlgNode visit( LogicalDocumentTransformer transformer ) {
        hashBasis.add( LogicalDocumentTransformer.class.getSimpleName() + "#" + transformer.inModelTrait.dataModel().name() + "#" + transformer.outModelTrait.dataModel().name() );
        return visitChildren( transformer );
    }


    @Override
    public AlgNode visit( LogicalConstraintEnforcer enforcer ) {
        hashBasis.add( LogicalConstraintEnforcer.class.getSimpleName() + "#" + enforcer.algCompareString() );
        return super.visit( enforcer );
    }


    @Override
    public AlgNode visit( LogicalRelMatch match ) {
        hashBasis.add( LogicalRelMatch.class.getSimpleName() + "#" + match.getEntity().id );
        return visitChild( match, 0, match.getInput() );
    }


    @Override
    public AlgNode visit( LogicalRelScan scan ) {
        if ( scan.getEntity() == null ) {
            throw new RuntimeException();
        }
        hashBasis.add( "Scan#" + scan.getEntity().id );

        addScannedEntity( scan.getEntity().dataModel, getLogicalId( scan ) );

        // get available columns for every table relScan
        this.getAvailableColumns( scan );

        return super.visit( scan );
    }


    private static long getLogicalId( Scan<?> scan ) {
        return scan.entity.isLogical() ? scan.entity.id : scan.entity.unwrap( AllocationEntity.class ).orElseThrow().getLogicalId();
    }


    private static long getLogicalId( Modify<?> modify ) {
        return modify.entity.isLogical() ? modify.entity.id : modify.entity.unwrap( AllocationEntity.class ).orElseThrow().getLogicalId();
    }


    @Override
    public AlgNode visit( LogicalRelFilter filter ) {
        hashBasis.add( LogicalRelFilter.class.getSimpleName() );
        super.visit( filter );
        filter.accept( this.rexShuttle );

        getPartitioningInfo( filter );

        return filter;
    }


    @Override
    public AlgNode visit( LogicalRelProject project ) {
        hashBasis.add( LogicalRelProject.class.getSimpleName() + "#" + project.getProjects().size() );
        super.visit( project );
        project.accept( this.rexShuttle );
        return project;
    }


    @Override
    public AlgNode visit( LogicalRelCorrelate correlate ) {
        hashBasis.add( LogicalRelCorrelate.class.getSimpleName() );
        return visitChildren( correlate );
    }


    @Override
    public AlgNode visit( LogicalRelJoin join ) {
        if ( join.getLeft() instanceof LogicalRelScan && join.getRight() instanceof LogicalRelScan ) {
            hashBasis.add( LogicalRelJoin.class + "#" + join.getLeft().getEntity().id + "#" + join.getRight().getEntity().id );
        }

        super.visit( join );
        join.accept( this.rexShuttle );
        return join;
    }


    @Override
    public AlgNode visit( LogicalRelUnion union ) {
        hashBasis.add( LogicalRelUnion.class.getSimpleName() );
        super.visit( union );
        union.accept( this.rexShuttle );
        return union;
    }


    @Override
    public AlgNode visit( LogicalRelIntersect intersect ) {
        hashBasis.add( LogicalRelIntersect.class.getSimpleName() );
        super.visit( intersect );
        intersect.accept( this.rexShuttle );
        return intersect;
    }


    @Override
    public AlgNode visit( LogicalRelMinus minus ) {
        hashBasis.add( LogicalRelMinus.class.getSimpleName() );
        return visitChildren( minus );
    }


    @Override
    public AlgNode visit( LogicalRelSort sort ) {
        hashBasis.add( LogicalRelSort.class.getSimpleName() );
        return visitChildren( sort );
    }


    @Override
    public AlgNode visit( LogicalRelExchange exchange ) {
        hashBasis.add( LogicalRelExchange.class.getSimpleName() + "#" + exchange.distribution.getType().shortName );
        return visitChildren( exchange );
    }


    @Override
    public AlgNode visit( LogicalRelModify modify ) {
        hashBasis.add( LogicalRelModify.class.getSimpleName() );

        addModifiedEntity( modify.getEntity().dataModel, getLogicalId( modify ) );

        // e.g. inserts only have underlying values and need to attach the table correctly
        this.getAvailableColumns( modify );
        return visitChildren( modify );
    }


    @Override
    public AlgNode visit( AlgNode other ) {
        hashBasis.add( "other#" + other.getClass().getSimpleName() );
        return visitChildren( other );
    }


    private void getAvailableColumns( AlgNode scan ) {

        if ( scan.getEntity().unwrap( LogicalTable.class ).isPresent() ) {
            final LogicalTable table = scan.getEntity().unwrap( LogicalTable.class ).get();
            final List<LogicalColumn> columns = Catalog.getInstance().getSnapshot().rel().getColumns( table.id );
            final List<String> names = columns.stream().map( c -> c.name ).toList();
            final String baseName = Catalog.getInstance().getSnapshot().getNamespace( table.namespaceId ) + "." + table.name + ".";

            for ( int i = 0; i < columns.size(); i++ ) {
                this.availableColumns.putIfAbsent( columns.get( i ).id, baseName + names.get( i ) );
                this.availableColumnsWithTable.putIfAbsent( columns.get( i ).id, table.id );
            }
        }
    }


    private void getPartitioningInfo( LogicalRelFilter filter ) {
        Entity table = filter.getInput().getEntity();
        if ( table == null || table.unwrap( LogicalTable.class ).isEmpty() ) {
            return;
        }

        handleIfPartitioned( filter, table.unwrap( LogicalTable.class ).orElseThrow() );
    }


    private void handleIfPartitioned( AlgNode node, Entity logicalEntity ) {
        // Only if table is partitioned
        if ( Catalog.snapshot().alloc().getPlacementsFromLogical( logicalEntity.id ).size() > 1
                || Catalog.snapshot().alloc().getPartitionsFromLogical( logicalEntity.id ).size() > 1 ) {
            WhereClauseVisitor whereClauseVisitor = new WhereClauseVisitor(
                    this.statement,
                    Catalog.snapshot().rel().getColumns( logicalEntity.id ).stream().map( c -> c.id ).toList().indexOf( Catalog.snapshot().alloc().getPartitionProperty( logicalEntity.id ).orElseThrow().partitionColumnId ) );
            node.accept( whereClauseVisitor );

            long scanId = node.getInput( 0 ).getEntity().id;

            if ( !partitionValueFilterPerScan.containsKey( scanId ) ) {
                partitionValueFilterPerScan.put( scanId, new HashSet<>() );
            }

            if ( whereClauseVisitor.valueIdentified ) {
                if ( !whereClauseVisitor.getValues().isEmpty() && !whereClauseVisitor.isUnsupportedFilter() ) {
                    partitionValueFilterPerScan.get( scanId ).addAll( whereClauseVisitor.getValues().stream()
                            .map( Object::toString )
                            .collect( Collectors.toSet() ) );
                }
            }
        }
    }


    private void getPartitioningInfo( LogicalDocumentFilter filter ) {
        Entity entity = filter.getInput().getEntity();
        if ( entity == null ) {
            return;
        }

        handleIfPartitioned( filter, entity );
    }


    private void getPartitioningInfo( LogicalLpgFilter filter ) {
        Entity entity = filter.getInput().getEntity();
        if ( entity == null ) {
            return;
        }

        handleIfPartitioned( filter, entity );

    }

}

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
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
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
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.prepare.AlgOptTableImpl;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.Statement;


/**
 * Universal routing alg shuttle class to extract partition and column information from AlgNode.
 */
public class LogicalAlgAnalyzeShuttle extends AlgShuttleImpl {

    protected final LogicalAlgAnalyzeRexShuttle rexShuttle;
    @Getter
    //protected final Map<Integer, List<String>> filterMap = new HashMap<>(); // logical scanId (TableScanId) -> List partitionsValue
    protected final Map<Integer, Set<String>> partitionValueFilterPerScan = new HashMap<>(); // logical scanId (TableScanId) -> (logical tableId -> List partitionsValue)
    @Getter
    protected final HashSet<String> hashBasis = new HashSet<>();
    @Getter
    protected final LinkedHashMap<Long, String> availableColumns = new LinkedHashMap<>(); // column id -> schemaName.tableName.ColumnName
    protected final HashMap<Long, Long> availableColumnsWithTable = new HashMap<>(); // columnId -> tableId
    @Getter
    protected final List<String> tables = new ArrayList<>();
    private final Statement statement;

    @Getter
    protected HashMap<Long, List<Object>> ordered;

    @Getter
    public int rowCount;


    public LogicalAlgAnalyzeShuttle( Statement statement ) {
        this.statement = statement;
        this.rexShuttle = new LogicalAlgAnalyzeRexShuttle();
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


    @Override
    public AlgNode visit( LogicalAggregate aggregate ) {
        hashBasis.add( "LogicalAggregate#" + aggregate.getAggCallList() );
        return visitChild( aggregate, 0, aggregate.getInput() );
    }


    @Override
    public AlgNode visit( LogicalMatch match ) {
        hashBasis.add( "LogicalMatch#" + match.getTable().getQualifiedName() );
        return visitChild( match, 0, match.getInput() );
    }


    @Override
    public AlgNode visit( TableScan scan ) {
        hashBasis.add( "TableScan#" + scan.getTable().getQualifiedName() );
        // get available columns for every table scan
        this.getAvailableColumns( scan );

        return super.visit( scan );
    }


    @Override
    public AlgNode visit( LogicalFilter filter ) {
        hashBasis.add( "LogicalFilter" );
        super.visit( filter );
        filter.accept( this.rexShuttle );

        getPartitioningInfo( filter );

        return filter;
    }


    @Override
    public AlgNode visit( LogicalProject project ) {
        hashBasis.add( "LogicalProject#" + project.getProjects().size() );
        super.visit( project );
        project.accept( this.rexShuttle );
        return project;
    }


    @Override
    public AlgNode visit( LogicalCorrelate correlate ) {
        hashBasis.add( "LogicalCorrelate" );
        return visitChildren( correlate );
    }


    @Override
    public AlgNode visit( LogicalJoin join ) {
        if ( join.getLeft() instanceof LogicalTableScan && join.getRight() instanceof LogicalTableScan ) {
            hashBasis.add( "LogicalJoin#" + join.getLeft().getTable().getQualifiedName() + "#" + join.getRight().getTable().getQualifiedName() );
        }

        super.visit( join );
        join.accept( this.rexShuttle );
        return join;
    }


    @Override
    public AlgNode visit( LogicalUnion union ) {
        hashBasis.add( "LogicalUnion" );
        super.visit( union );
        union.accept( this.rexShuttle );
        return union;
    }


    @Override
    public AlgNode visit( LogicalIntersect intersect ) {
        hashBasis.add( "LogicalIntersect" );
        super.visit( intersect );
        intersect.accept( this.rexShuttle );
        return intersect;
    }


    @Override
    public AlgNode visit( LogicalMinus minus ) {
        hashBasis.add( "LogicalMinus" );
        return visitChildren( minus );
    }


    @Override
    public AlgNode visit( LogicalSort sort ) {
        hashBasis.add( "LogicalSort" );
        return visitChildren( sort );
    }


    @Override
    public AlgNode visit( LogicalExchange exchange ) {
        hashBasis.add( "LogicalExchange#" + exchange.distribution.getType().shortName );
        return visitChildren( exchange );
    }


    @Override
    public AlgNode visit( AlgNode other ) {
        hashBasis.add( "other#" + other.getClass().getSimpleName() );
        if ( other instanceof LogicalTableModify ) {
            // Add all columns to availableColumnsWithTable for statistics
            if ( (other.getTable().getTable() instanceof LogicalTable) ) {
                LogicalTable logicalTable = ((LogicalTable) other.getTable().getTable());
                Long tableId = logicalTable.getTableId();
                logicalTable.getColumnIds().forEach( v -> availableColumnsWithTable.put( v, tableId ) );
            }
        }
        return visitChildren( other );
    }


    private void getAvailableColumns( TableScan scan ) {
        this.tables.addAll( scan.getTable().getQualifiedName() );
        final Table table = scan.getTable().getTable();
        LogicalTable logicalTable = (table instanceof LogicalTable) ? (LogicalTable) table : null;
        if ( logicalTable != null ) {
            final List<Long> ids = logicalTable.getColumnIds();
            final List<String> names = logicalTable.getLogicalColumnNames();
            final String baseName = logicalTable.getLogicalSchemaName() + "." + logicalTable.getLogicalTableName() + ".";

            for ( int i = 0; i < ids.size(); i++ ) {
                this.availableColumns.putIfAbsent( ids.get( i ), baseName + names.get( i ) );
                this.availableColumnsWithTable.putIfAbsent( ids.get( i ), logicalTable.getTableId() );
            }
        }
    }


    private void getPartitioningInfo( LogicalFilter filter ) {
        AlgOptTableImpl table = (AlgOptTableImpl) filter.getInput().getTable();
        if ( table == null ) {
            return;
        }

        final Table logicalTable = table.getTable();
        if ( !(logicalTable instanceof LogicalTable) ) {
            return;
        }
        CatalogTable catalogTable = Catalog.getInstance().getTable( ((LogicalTable) logicalTable).getTableId() );

        // Only if table is partitioned
        if ( catalogTable.partitionProperty.isPartitioned ) {
            WhereClauseVisitor whereClauseVisitor = new WhereClauseVisitor(
                    statement,
                    catalogTable.columnIds.indexOf( catalogTable.partitionProperty.partitionColumnId ) );
            filter.accept( whereClauseVisitor );

            int scanId = filter.getInput().getId();

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

}

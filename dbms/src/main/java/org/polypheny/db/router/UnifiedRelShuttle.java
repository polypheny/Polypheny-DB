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

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.val;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelShuttleImpl;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalIntersect;
import org.polypheny.db.rel.logical.LogicalJoin;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.logical.LogicalUnion;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.transaction.Statement;

/**
 * Unified routing rel shuttle class to extract used columns from RelNode.
 */
public class UnifiedRelShuttle extends RelShuttleImpl {

    protected final UnifiedRexShuttle rexShuttle;
    private final Statement statement;

    @Getter
    protected final LinkedHashMap<Long, String> availableColumns = new LinkedHashMap<>(); // column id -> schemaName.tableName.ColumnName

    public Map<Long, String> getUsedColumns() {

        return this.availableColumns;

        /*val availableColumnNames = new ArrayList<String>(this.availableColumns.values());
        val availableColumnKeys = new ArrayList<Long>(this.availableColumns.keySet());

        if(this.rexShuttle.ids.isEmpty()){
            return this.availableColumns;
        }

        val usedColumns = this.rexShuttle.ids.stream()
                .collect( Collectors.toMap(
                        index -> availableColumnKeys.get( index ),
                        index -> availableColumnNames.get( index ) ) );
        return usedColumns;*/
    }

    public UnifiedRelShuttle( Statement s ) {
        this.statement = s;
        rexShuttle = new UnifiedRexShuttle( this.statement );
    }


    @Override
    public RelNode visit( TableScan scan ) {
        // get available columns for every table scan
        LogicalTable table = (LogicalTable) ((RelOptTableImpl) scan.getTable()).getTable();
        if ( table != null ) {
            val ids = table.getColumnIds();
            val names = table.getLogicalColumnNames();
            val baseName = table.getLogicalSchemaName() + "." + table.getLogicalTableName() + ".";

            for ( int i = 0; i < ids.size(); i++ ) {
                this.availableColumns.putIfAbsent( ids.get( i ), baseName + names.get( i ) );
            }
        }

        return super.visit( scan );
    }


    @Override
    public RelNode visit( LogicalFilter filter ) {
        super.visit( filter );
        filter.accept( this.rexShuttle );
        return filter;
    }


    @Override
    public RelNode visit( LogicalProject project ) {
        super.visit( project );
        project.accept( this.rexShuttle );
        return project;
    }


    @Override
    public RelNode visit( LogicalJoin join ) {
        super.visit( join );
        join.accept( this.rexShuttle );
        return join;
    }


    @Override
    public RelNode visit( LogicalUnion union ) {
        super.visit( union );
        union.accept( this.rexShuttle );
        return union;
    }


    @Override
    public RelNode visit( LogicalIntersect intersect ) {
        super.visit( intersect );
        intersect.accept( this.rexShuttle );
        return intersect;
    }

}


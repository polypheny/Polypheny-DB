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

package org.polypheny.db.adaptimizer.randomdata;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.polypheny.db.adaptimizer.randomdata.except.TestDataGenerationException;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.util.Pair;

public class DataTableOptionTemplate {

    private final CatalogTable catalogTable;
    private final List<CatalogColumn> catalogColumns;
    private final List<DataColumnOption> dataColumnOptions;
    private final List<Long> columnIds;

    private final int size;

    private final Set<Pair<Long, Integer>> referenced;
    private final List<Pair<Long, Long>> references;


    public DataTableOptionTemplate( CatalogTable catalogTable, List<CatalogColumn> columns, List<DataColumnOption> options, int size ) {

        if ( columns.size() != options.size() ) {
            throw new TestDataGenerationException( "Not all provided columns have corresponding options.", new IllegalArgumentException() );
        }

        this.catalogTable = catalogTable;
        this.catalogColumns = columns;
        this.dataColumnOptions = options;
        this.columnIds = columns.stream().map( column -> column.id ).collect( Collectors.toList());
        this.size = size;

        this.references = new LinkedList<>();
        this.referenced = new HashSet<>();
    }

    public int getSize() {
        return this.size;
    }

    public long getTableId() {
        return this.catalogTable.id;
    }

    public boolean hasTableId( long id ) {
        return this.catalogTable.id == id;
    }

    public CatalogTable getCatalogTable() {
        return this.catalogTable;
    }

    public List<CatalogColumn> getCatalogColumns() {
        return this.catalogColumns;
    }

    public List<Long> getColumnIds() {
        return this.columnIds;
    }

    public void addReferencingColumns( List<Long> referencingColumns, List<Long> referencedColumns ) {
        for ( int i = 0; i < referencedColumns.size(); i++ ) {
            this.references.add( new Pair<>( referencingColumns.get( i ), referencedColumns.get( i ) ) );
        }
    }

    public void addReferencedColumnIndexes( List<Long> referencedColumns ) {
        for ( Long referencedColumn : referencedColumns ) {
            this.referenced.add( new Pair<>( referencedColumn, this.columnIds.indexOf( referencedColumn ) ) );
        }
    }

    public Set<Pair<Long, Integer>> getReferencedColumnIds() {
        return this.referenced;
    }

    public List<Pair<Long, Long>> getReferences() {
        return this.references;
    }

    public Map<Long, DataColumnOption> getOptions() {
        return IntStream.range(0, this.catalogColumns.size()).boxed().collect(
                Collectors.toMap(
                        this.catalogColumns.stream().map( catalogColumn -> catalogColumn.id ).collect( Collectors.toList())::get,
                        this.dataColumnOptions.stream().map( DataColumnOption::fromTemplate ).collect( Collectors.toList() )::get
                )
        );
    }

}

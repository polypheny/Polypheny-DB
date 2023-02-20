/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.catalog.snapshot.logical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Value;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.logical.relational.CatalogColumn;
import org.polypheny.db.catalog.logical.relational.CatalogSchema;
import org.polypheny.db.catalog.logical.relational.CatalogTable;
import org.polypheny.db.catalog.logical.relational.RelationalCatalog;

@Value
public class LogicalRelationalSnapshot implements LogicalSnapshot {

    ImmutableList<RelationalCatalog> catalogs;

    public ImmutableList<CatalogSchema> schemas;
    public ImmutableMap<Long, CatalogSchema> schemaIds;
    public ImmutableMap<String, CatalogSchema> schemaNames;
    public ImmutableList<CatalogTable> tables;
    public ImmutableMap<Long, CatalogTable> tableIds;
    public ImmutableMap<String, CatalogTable> tableNames;
    public ImmutableList<CatalogColumn> columns;
    public ImmutableMap<Long, CatalogColumn> columnIds;
    public ImmutableMap<String, CatalogColumn> columnNames;


    public LogicalRelationalSnapshot( List<RelationalCatalog> catalogs ) {
        this.catalogs = ImmutableList.copyOf( catalogs.stream().map( RelationalCatalog::copy ).collect( Collectors.toList() ) );

        this.schemas = ImmutableList.copyOf( buildSchemas() );
        this.schemaIds = ImmutableMap.copyOf( buildSchemaIds() );
        this.schemaNames = ImmutableMap.copyOf( buildSchemaNames() );

        this.tables = ImmutableList.copyOf( buildTables() );
        this.tableIds = ImmutableMap.copyOf( buildTableIds() );
        this.tableNames = ImmutableMap.copyOf( buildTableNames() );

        this.columns = ImmutableList.copyOf( buildColumns() );
        this.columnIds = ImmutableMap.copyOf( buildColumnIds() );
        this.columnNames = ImmutableMap.copyOf( buildColumnNames() );
    }

    ///////////////////////////
    ///// Columns /////////////
    ///////////////////////////


    private List<CatalogColumn> buildColumns() {
        return tables.stream().flatMap( t -> t.columns.values().stream() ).collect( Collectors.toList() );
    }


    private Map<Long, CatalogColumn> buildColumnIds() {
        return columns.stream().collect( Collectors.toMap( c -> c.id, c -> c ) );
    }


    private Map<String, CatalogColumn> buildColumnNames() {
        return columns.stream().collect( Collectors.toMap( c -> c.name, c -> c ) );
    }

    ///////////////////////////
    ///// Tables //////////////
    ///////////////////////////


    private List<CatalogTable> buildTables() {
        return catalogs.stream().flatMap( c -> c.tables.values().stream() ).collect( Collectors.toList() );
    }


    private Map<Long, CatalogTable> buildTableIds() {
        return tables.stream().collect( Collectors.toMap( c -> c.id, c -> c ) );
    }


    private Map<String, CatalogTable> buildTableNames() {
        return tables.stream().collect( Collectors.toMap( c -> c.name, c -> c ) );
    }

    ///////////////////////////
    ///// Schema //////////////
    ///////////////////////////


    private List<CatalogSchema> buildSchemas() {
        return catalogs.stream().map( c -> new CatalogSchema( c.id, c.name ) ).collect( Collectors.toList() );
    }


    private Map<Long, CatalogSchema> buildSchemaIds() {
        return schemas.stream().collect( Collectors.toMap( c -> c.id, c -> c ) );
    }


    private Map<String, CatalogSchema> buildSchemaNames() {
        return schemas.stream().collect( Collectors.toMap( c -> c.name, c -> c ) );
    }


    @Override
    public NamespaceType getType() {
        return NamespaceType.RELATIONAL;
    }

}

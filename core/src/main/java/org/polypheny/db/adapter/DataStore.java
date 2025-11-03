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

package org.polypheny.db.adapter;


import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.ExtensionPoint;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.AdapterCatalog;
import org.polypheny.db.catalog.entity.logical.LogicalTable;

@Slf4j
public abstract class DataStore<S extends AdapterCatalog> extends Adapter<S> implements Modifiable, ExtensionPoint {

    @Getter
    private final boolean persistent;

    protected final transient Catalog catalog = Catalog.getInstance();


    public DataStore( final long adapterId, final String uniqueName, final Map<String, String> settings, final DeployMode mode, final boolean persistent, S storeCatalog ) {
        super( adapterId, uniqueName, settings, mode, storeCatalog );
        this.persistent = persistent;

        informationPage.setLabel( "Stores" );
    }


    public abstract List<IndexMethodModel> getAvailableIndexMethods();

    public abstract IndexMethodModel getDefaultIndexMethod();

    public abstract List<FunctionalIndexInfo> getFunctionalIndexes( LogicalTable catalogTable );


    public record IndexMethodModel( @JsonProperty String name, @JsonProperty String displayName ) {

    }


    public record FunctionalIndexInfo( List<Long> columnIds, String methodDisplayName ) {

        public List<String> getColumnNames() {
            List<String> columnNames = new ArrayList<>( columnIds.size() );
            for ( long columnId : columnIds ) {
                columnNames.add( Catalog.snapshot().rel().getColumn( columnId ).orElseThrow().name );
            }
            return columnNames;
        }

    }

}

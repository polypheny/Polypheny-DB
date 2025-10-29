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

import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.pf4j.ExtensionPoint;
import org.polypheny.db.catalog.catalogs.AdapterCatalog;
import org.polypheny.db.catalog.logistic.DataModel;

@Getter
public abstract class DataSource<S extends AdapterCatalog> extends Adapter<S> implements ExtensionPoint {

    private final Set<DataModel> supportedDataModels;
    private final boolean dataReadOnly;


    protected DataSource( final long adapterId, final String uniqueName, final Map<String, String> settings, final DeployMode mode, boolean dataReadOnly, S catalog, Set<DataModel> supportedModels ) {
        super( adapterId, uniqueName, settings, mode, catalog );
        this.dataReadOnly = dataReadOnly;
        this.supportedDataModels = Set.copyOf( supportedModels );
        informationPage.setLabel( "Sources" );
    }


    public boolean supportsRelational() {
        return supportedDataModels.contains( DataModel.RELATIONAL );
    }


    public boolean supportsDocument() {
        return supportedDataModels.contains( DataModel.DOCUMENT );
    }


    public boolean supportsGraph() {
        return supportedDataModels.contains( DataModel.GRAPH );
    }


    public RelationalDataSource asRelationalDataSource() {
        // should be overridden by subclasses accordingly
        throw new IllegalStateException( "This source does not support the relational data model." );
    }


    public DocumentDataSource asDocumentDataSource() {
        // should be overridden by subclasses accordingly
        throw new IllegalStateException( "This source does not support the document data model." );
    }


    public DocumentDataSource asGraphDataSource() {
        // should be overridden by subclasses accordingly
        throw new IllegalStateException( "This source does not support the graph data model." );
    }

}

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

package org.polypheny.db.catalog.logical.graph;

import io.activej.serializer.BinarySerializer;
import java.util.List;
import lombok.Getter;
import lombok.Value;
import lombok.With;
import lombok.experimental.NonFinal;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.NCatalog;
import org.polypheny.db.catalog.Serializable;
import org.polypheny.db.catalog.catalogs.LogicalGraphCatalog;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.logistic.Pattern;

@Value
@With
public class GraphCatalog implements NCatalog, Serializable, LogicalGraphCatalog {

    @Getter
    public BinarySerializer<GraphCatalog> serializer = Serializable.builder.get().build( GraphCatalog.class );
    @Getter
    public LogicalNamespace logicalNamespace;
    public IdBuilder idBuilder;


    @NonFinal
    boolean openChanges = false;


    public GraphCatalog( LogicalNamespace logicalNamespace, IdBuilder idBuilder ) {

        this.logicalNamespace = logicalNamespace;
        this.idBuilder = idBuilder;
    }


    @Override
    public void commit() {
        openChanges = false;
    }


    @Override
    public void rollback() {

        openChanges = false;
    }


    @Override
    public boolean hasUncommittedChanges() {
        return openChanges;
    }


    @Override
    public NamespaceType getType() {
        return NamespaceType.GRAPH;
    }


    @Override
    public GraphCatalog copy() {
        return deserialize( serialize(), GraphCatalog.class );
    }


    @Override
    public boolean checkIfExistsEntity( String entityName ) {
        return false;
    }


    @Override
    public boolean checkIfExistsEntity( long tableId ) {
        return false;
    }


    @Override
    public void addGraphAlias( long graphId, String alias, boolean ifNotExists ) {

    }


    @Override
    public void removeGraphAlias( long graphId, String alias, boolean ifExists ) {

    }


    @Override
    public long addGraph( String name, List<DataStore> stores, boolean modifiable, boolean ifNotExists, boolean replace ) {
        return 0;
    }


    @Override
    public void deleteGraph( long id ) {

    }


    @Override
    public LogicalGraph getGraph( long id ) {
        return null;
    }


    @Override
    public List<LogicalGraph> getGraphs( Pattern graphName ) {
        return null;
    }


    @Override
    public void addGraphLogistics( long id, List<DataStore> stores, boolean onlyPlacement ) throws GenericCatalogException, UnknownTableException, UnknownColumnException {

    }

}

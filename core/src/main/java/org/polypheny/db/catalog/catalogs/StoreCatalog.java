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

package org.polypheny.db.catalog.catalogs;


import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.tools.AlgBuilder;

@Value
@NonFinal
public abstract class StoreCatalog {

    @Serialize
    public long adapterId;
    IdBuilder idBuilder = IdBuilder.getInstance();
    public ConcurrentMap<Long, Namespace> namespaces;


    public StoreCatalog(
            @Deserialize("adapterId") long adapterId ) {
        this( new HashMap<>(), adapterId );
    }


    public StoreCatalog(
            @Deserialize("namespaces") Map<Long, Namespace> namespaces,
            @Deserialize("adapterId") long adapterId ) {
        this.adapterId = adapterId;
        this.namespaces = new ConcurrentHashMap<>( namespaces );
    }


    public Expression asExpression() {
        return Expressions.call( Catalog.CATALOG_EXPRESSION, "getStoreSnapshot", Expressions.constant( adapterId ) );
    }


    public abstract AlgNode getRelScan( long allocId, AlgBuilder statement );

    public abstract AlgNode getGraphScan( long allocId, AlgBuilder statement );

    public abstract AlgNode getDocumentScan( long allocId, AlgBuilder statement );

    public abstract AlgNode getScan( long id, AlgBuilder builder );


    public void addNamespace( long namespaceId, Namespace namespace ) {
        this.namespaces.put( namespaceId, namespace );
    }


    public void remove( long namespaceId ) {
        this.namespaces.remove( namespaceId );
    }


    public Namespace getNamespace( long id ) {
        return namespaces.get( id );
    }

}

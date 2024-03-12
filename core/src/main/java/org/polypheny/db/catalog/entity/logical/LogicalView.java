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

package org.polypheny.db.catalog.entity.logical;


import com.google.common.collect.ImmutableMap;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.Serial;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.BiAlg;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.logical.relational.LogicalRelViewScan;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.view.ViewManager.ViewVisitor;

@EqualsAndHashCode(callSuper = true)
@SuperBuilder(toBuilder = true)
@Value
@NonFinal
public class LogicalView extends LogicalTable {

    @Serial
    private static final long serialVersionUID = -4771308114962700515L;

    @Serialize
    public ImmutableMap<Long, List<Long>> underlyingTables;
    @Serialize
    public QueryLanguage language;
    @Serialize
    public AlgCollation algCollation;
    @Serialize
    public String query;


    public LogicalView(
            @Deserialize("id") long id,
            @Deserialize("name") String name,
            @Deserialize("namespaceId") long namespaceId,
            @Deserialize("entityType") EntityType entityType,
            @Deserialize("query") String query,
            @Deserialize("algCollation") AlgCollation algCollation,
            @Deserialize("underlyingTables") Map<Long, List<Long>> underlyingTables,
            @Deserialize("language") QueryLanguage language ) {
        super(
                id,
                name,
                namespaceId,
                entityType,
                null,
                false );
        this.query = query;
        this.algCollation = algCollation;
        this.underlyingTables = ImmutableMap.copyOf( underlyingTables );
        this.language = language;
    }


    public AlgNode prepareView( AlgCluster cluster ) {
        AlgNode viewLogicalRoot = getDefinition();
        prepareView( viewLogicalRoot, cluster );

        ViewVisitor materializedVisitor = new ViewVisitor( false );
        viewLogicalRoot.accept( materializedVisitor );

        return viewLogicalRoot;
    }


    public void prepareView( AlgNode viewLogicalRoot, AlgCluster algCluster ) {
        if ( viewLogicalRoot instanceof AbstractAlgNode ) {
            ((AbstractAlgNode) viewLogicalRoot).setCluster( algCluster );
        }
        if ( viewLogicalRoot instanceof BiAlg ) {
            prepareView( ((BiAlg) viewLogicalRoot).getLeft(), algCluster );
            prepareView( ((BiAlg) viewLogicalRoot).getRight(), algCluster );
        } else if ( viewLogicalRoot instanceof SingleAlg ) {
            prepareView( ((SingleAlg) viewLogicalRoot).getInput(), algCluster );
        }
        if ( viewLogicalRoot instanceof LogicalRelViewScan ) {
            prepareView( ((LogicalRelViewScan) viewLogicalRoot).getAlgNode(), algCluster );
        }
    }


    public AlgNode getDefinition() {
        return Catalog.snapshot().rel().getNodeInfo( id );
    }

}

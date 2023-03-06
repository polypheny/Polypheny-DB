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

package org.polypheny.db.catalog.entity;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.With;
import lombok.experimental.NonFinal;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.BiAlg;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.logical.relational.LogicalRelViewScan;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.view.ViewManager.ViewVisitor;

@EqualsAndHashCode(callSuper = true)
@With
@Value
@NonFinal
public class CatalogView extends LogicalTable {

    private static final long serialVersionUID = -4771308114962700515L;

    public ImmutableMap<Long, ImmutableList<Long>> underlyingTables;
    public String language;
    public AlgCollation algCollation;
    public String query;


    public CatalogView(
            long id,
            String name,
            String namespaceName,
            List<LogicalColumn> columns,
            long namespaceId,
            EntityType entityType,
            String query,
            Long primaryKey,
            List<Long> dataPlacements,
            boolean modifiable,
            PartitionProperty partitionProperty,
            AlgCollation algCollation,
            List<Long> connectedViews,
            Map<Long, List<Long>> underlyingTables,
            String language ) {
        super(
                id,
                name,
                columns,
                namespaceId,
                namespaceName,
                entityType,
                primaryKey,
                dataPlacements,
                modifiable,
                partitionProperty,
                connectedViews );
        this.query = query;
        this.algCollation = algCollation;
        this.underlyingTables = ImmutableMap.copyOf( underlyingTables.entrySet().stream().collect( Collectors.toMap( Entry::getKey, t -> ImmutableList.copyOf( t.getValue() ) ) ) );
        // mapdb cannot handle the class QueryLanguage, therefore we use the String here
        this.language = language;
    }


    public QueryLanguage getLanguage() {
        return QueryLanguage.from( language );
    }


    public AlgNode prepareView( AlgOptCluster cluster ) {
        AlgNode viewLogicalRoot = getDefinition();
        prepareView( viewLogicalRoot, cluster );

        ViewVisitor materializedVisitor = new ViewVisitor( false );
        viewLogicalRoot.accept( materializedVisitor );

        return viewLogicalRoot;
    }


    public void prepareView( AlgNode viewLogicalRoot, AlgOptCluster algOptCluster ) {
        if ( viewLogicalRoot instanceof AbstractAlgNode ) {
            ((AbstractAlgNode) viewLogicalRoot).setCluster( algOptCluster );
        }
        if ( viewLogicalRoot instanceof BiAlg ) {
            prepareView( ((BiAlg) viewLogicalRoot).getLeft(), algOptCluster );
            prepareView( ((BiAlg) viewLogicalRoot).getRight(), algOptCluster );
        } else if ( viewLogicalRoot instanceof SingleAlg ) {
            prepareView( ((SingleAlg) viewLogicalRoot).getInput(), algOptCluster );
        }
        if ( viewLogicalRoot instanceof LogicalRelViewScan ) {
            prepareView( ((LogicalRelViewScan) viewLogicalRoot).getAlgNode(), algOptCluster );
        }
    }


    public AlgNode getDefinition() {
        return Catalog.getInstance().getNodeInfo().get( id );
    }

}

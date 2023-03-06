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

import io.activej.serializer.annotations.Serialize;
import java.io.Serializable;
import java.util.List;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.StatisticsManager;
import org.polypheny.db.algebra.AlgDistribution;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.refactor.CatalogType;
import org.polypheny.db.catalog.refactor.Expressible;
import org.polypheny.db.plan.AlgMultipleTrait;
import org.polypheny.db.schema.Statistic;
import org.polypheny.db.schema.Wrapper;
import org.polypheny.db.util.ImmutableBitSet;

@SuperBuilder(toBuilder = true)
@Value
@NonFinal
public abstract class CatalogEntity implements CatalogObject, Wrapper, Serializable, CatalogType, Expressible {
    @Serialize
    public long id;
    @Serialize
    public EntityType entityType;
    @Serialize
    public NamespaceType namespaceType;
    @Serialize
    public String name;
    @Serialize
    public long namespaceId;


    protected CatalogEntity( long id, String name, long namespaceId, EntityType type, NamespaceType namespaceType ) {
        this.id = id;
        this.namespaceId = namespaceId;
        this.name = name;
        this.entityType = type;
        this.namespaceType = namespaceType;
    }


    public AlgDataType getRowType() {
        switch ( namespaceType ) {
            case RELATIONAL:
                throw new UnsupportedOperationException( "Should be overwritten by child" );
            case DOCUMENT:
                return new DocumentType();
            case GRAPH:
                return new GraphType();
        }
        throw new RuntimeException( "Error while generating the RowType" );
    }


    @Deprecated
    public boolean rolledUpColumnValidInsideAgg() {
        return true;
    }


    @Deprecated
    public boolean isRolledUp( String fieldName ) {
        return false;
    }


    public double getRowCount() {
        Integer count = StatisticsManager.getInstance().rowCountPerTable( id );
        if ( count == null ) {
            return 0;
        }
        return count;
    }


    public <T extends AlgMultipleTrait> List<T> getCollations() {
        return null;
    }


    public Boolean isKey( ImmutableBitSet columns ) {
        return null;
    }


    public AlgDistribution getDistribution() {
        return null;
    }


    public Statistic getStatistic() {
        return null;
    }

}

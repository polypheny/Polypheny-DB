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

package org.polypheny.db.catalog.entity;

import io.activej.serializer.annotations.Serialize;
import java.io.Serializable;
import java.util.List;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.experimental.SuperBuilder;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.StatisticsManager;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgDistribution;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.refactor.CatalogType;
import org.polypheny.db.schema.Statistic;
import org.polypheny.db.schema.Statistics;
import org.polypheny.db.schema.types.Expressible;
import org.polypheny.db.schema.types.Typed;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Wrapper;

@Getter
@SuperBuilder(toBuilder = true)
@Value
@NonFinal
public abstract class Entity implements PolyObject, Wrapper, Serializable, CatalogType, Expressible, Typed, Comparable<Entity> {

    @Serialize
    public long id;

    @Serialize
    public EntityType entityType;

    @Serialize
    public DataModel dataModel;

    @Serialize
    public String name;

    @Serialize
    public long namespaceId;

    @Serialize
    public boolean modifiable;


    public Entity(
            long id,
            @NotNull String name,
            long namespaceId,
            EntityType type,
            DataModel dataModel,
            boolean modifiable ) {
        this.id = id;
        this.namespaceId = namespaceId;
        this.name = name;
        this.entityType = type;
        this.dataModel = dataModel;
        this.modifiable = modifiable;
    }


    public AlgDataType getTupleType() {
        return switch ( dataModel ) {
            case RELATIONAL -> throw new UnsupportedOperationException( "Should be overwritten by child" );
            case DOCUMENT -> DocumentType.ofId();
            case GRAPH -> GraphType.of();
        };
    }


    @Override
    public AlgDataType getTupleType( AlgDataTypeFactory typeFactory ) {
        return getTupleType();
    }


    @Deprecated
    public boolean rolledUpColumnValidInsideAgg() {
        return true;
    }


    @Deprecated
    public boolean isRolledUp( String fieldName ) {
        return false;
    }


    public double getTupleCount() {
        return getTupleCount( id );
    }


    public double getTupleCount( long id ) {
        Long count = StatisticsManager.getInstance().tupleCountPerEntity( id );
        if ( count == null ) {
            return 0;
        }
        return count;
    }


    public List<AlgCollation> getCollations() {
        return Statistics.UNKNOWN.getCollations();
    }


    public Boolean isKey( ImmutableBitSet fields ) {
        return null;
    }


    public AlgDistribution getDistribution() {
        return null;
    }


    public Statistic getStatistic() {
        return null;
    }


    public String getNamespaceName() {
        throw new UnsupportedOperationException( "Should be overwritten by child" );
    }


    @Override
    public int compareTo( @NotNull Entity o ) {
        if ( !this.getClass().getSimpleName().equals( o.getClass().getSimpleName() ) ) {
            return -1;
        }
        return (int) (this.id - o.id);
    }

}

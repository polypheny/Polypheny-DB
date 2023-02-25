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

import java.io.Serializable;
import java.util.List;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.StatisticsManager;
import org.polypheny.db.algebra.AlgDistribution;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.refactor.CatalogType;
import org.polypheny.db.catalog.refactor.Expressible;
import org.polypheny.db.plan.AlgMultipleTrait;
import org.polypheny.db.schema.Wrapper;
import org.polypheny.db.util.ImmutableBitSet;

@SuperBuilder(toBuilder = true)
public abstract class CatalogEntity implements CatalogObject, Wrapper, Serializable, CatalogType, Expressible {

    public final long id;
    public final EntityType entityType;
    public final NamespaceType namespaceType;
    public final String name;


    protected CatalogEntity( long id, String name, EntityType type, NamespaceType namespaceType ) {
        this.id = id;
        this.name = name;
        this.entityType = type;
        this.namespaceType = namespaceType;
    }


    public AlgDataType getRowType() {
        return null;
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

}

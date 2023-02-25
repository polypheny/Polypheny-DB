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

package org.polypheny.db.catalog.entity.physical;

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.EntityType;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.type.PolyTypeFactoryImpl;

public class PhysicalTable extends CatalogEntity implements Physical {

    public final ImmutableList<CatalogColumnPlacement> placements;


    protected PhysicalTable( long id, String name, EntityType type, NamespaceType namespaceType, List<CatalogColumnPlacement> placements ) {
        super( id, name, type, namespaceType );
        this.placements = ImmutableList.copyOf( placements );
    }


    public PhysicalTable( PhysicalTable table ) {
        this( table.id, table.name, table.entityType, table.namespaceType, table.placements );
    }


    public AlgProtoDataType buildProto() {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        final AlgDataTypeFactory.Builder fieldInfo = typeFactory.builder();

        for ( CatalogColumnPlacement placement : placements ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( placement.columnId );
            AlgDataType sqlType = catalogColumn.getAlgDataType( typeFactory );
            fieldInfo.add( catalogColumn.name, placement.physicalColumnName, sqlType ).nullable( catalogColumn.nullable );
        }

        return AlgDataTypeImpl.proto( fieldInfo.build() );
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }

}

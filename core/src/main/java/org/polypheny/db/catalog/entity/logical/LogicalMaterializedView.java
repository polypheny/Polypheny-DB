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

import com.google.common.collect.ImmutableList;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.languages.QueryLanguage;

@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
@Value
public class LogicalMaterializedView extends LogicalView {

    private static final long serialVersionUID = 4728996184367206274L;

    @Serialize
    public MaterializedCriteria materializedCriteria;
    @Serialize
    public boolean ordered;


    public LogicalMaterializedView(
            @Deserialize("id") long id,
            @Deserialize("name") String name,
            @Deserialize("namespaceId") long namespaceId,
            @Deserialize("entityType") String query,
            @Deserialize("algCollation") AlgCollation algCollation,
            @Deserialize("underlyingTables") Map<Long, List<Long>> underlyingTables,
            @Deserialize("language") QueryLanguage language,
            @Deserialize("materializedCriteria") MaterializedCriteria materializedCriteria,
            @Deserialize("ordered") boolean ordered
    ) {
        super(
                id,
                name,
                namespaceId,
                EntityType.MATERIALIZED_VIEW,
                query,
                algCollation,
                underlyingTables,
                language );

        Map<Long, ImmutableList<Long>> map = new HashMap<>();
        for ( Entry<Long, List<Long>> e : underlyingTables.entrySet() ) {
            if ( map.put( e.getKey(), ImmutableList.copyOf( e.getValue() ) ) != null ) {
                throw new IllegalStateException( "Duplicate key" );
            }
        }
        this.materializedCriteria = materializedCriteria;
        this.ordered = ordered;
    }


}

/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.postgres.store;

import org.polypheny.db.adapter.postgres.source.PostgresqlFeature;
import org.polypheny.db.sql.language.SqlDbFeature;
import java.util.Set;

public enum PostgresqlImageVariant {

    DEFAULT_OLD     ( "polypheny/postgres:latest", Set.of( PostgresqlFeature.POSTGIS ) ),

    DEFAULT         ( "polypheny/postgres:17-debian", Set.of() ),

    PGVECTOR        ( "polypheny/postgres-pgvector:17-debian", Set.of( PostgresqlFeature.PGVECTOR ) ),

    POSTGIS         ( "polypheny/postgres-postgis:17-debian", Set.of( PostgresqlFeature.POSTGIS ) ),

    PGVECTOR_POSTGIS( "polypheny/postgres-pgvector-postgis:17-debian", Set.of( PostgresqlFeature.PGVECTOR, PostgresqlFeature.POSTGIS ) );

    public final String imageName;
    public final Set<SqlDbFeature> features;

    PostgresqlImageVariant( String imageName, Set<SqlDbFeature> features ) {
        this.imageName = imageName;
        this.features = features;
    }

}

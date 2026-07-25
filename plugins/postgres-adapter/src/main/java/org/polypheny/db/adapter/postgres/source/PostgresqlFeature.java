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

package org.polypheny.db.adapter.postgres.source;

import org.polypheny.db.sql.language.SqlDbFeature;
import org.polypheny.db.sql.language.SqlDialect;
import java.util.function.Predicate;

public enum PostgresqlFeature implements SqlDbFeature {

    PGVECTOR( "vector", "pgvector", SqlDialect::supportsVector ),

    POSTGIS( "postgis", "PostGIS", SqlDialect::supportsPostGIS );

    /**
     * Name as it appears in {@code pg_extension.extname}.
     */
    private final String name;
    private final String displayName;
    private final Predicate<SqlDialect> supportCheck;

    PostgresqlFeature( String name, String displayName, Predicate<SqlDialect> supportCheck ) {
        this.name = name;
        this.displayName = displayName;
        this.supportCheck = supportCheck;
    }


    @Override
    public String featureName() {
        return name;
    }


    @Override
    public String displayName() {
        return displayName;
    }


    @Override
    public boolean isSupported( SqlDialect dialect ) {
        return this.supportCheck.test( dialect );
    }


    @Override
    public String getFeatureRegistrationQuery() {
        return "CREATE EXTENSION IF NOT EXISTS \"" + this.name + "\"";
    }
}

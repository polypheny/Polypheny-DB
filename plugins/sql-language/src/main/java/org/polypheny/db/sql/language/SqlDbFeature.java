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

package org.polypheny.db.sql.language;

/**
 * Represents a named feature of a SQL database that may or may not be available
 * on a given instance.
 *
 * <p>Examples of such features include database extensions (e.g. pgvector, PostGIS
 * in PostgreSQL), plugins (e.g. in MySQL), or any other optional capability that
 * is not guaranteed to be present on every instance of a database.
 *
 * <p>Each SQL adapter defines its own enum implementing this interface, enumerating
 * the features it knows how to detect. The detected set is passed to the
 * {@link SqlDialect}, which exposes individual capabilities
 * through methods such as {@link SqlDialect#supportsVector()} via
 * {@link SqlDialect#supportsFeature(SqlDbFeature)}.
 */
public interface SqlDbFeature {

    /**
     * Returns the name by which this feature is identified in the database's
     * own catalog or metadata system.
     *
     * @return the feature's catalog name, never {@code null}
     */
    String featureName();

    /**
     * Returns the human-readable name of this feature, intended for display in
     * user interfaces.
     *
     * @return the feature's display name, never {@code null}
     */
    String displayName();

    boolean isSupported( SqlDialect dialect );

    /**
     * Creates a query than can be run in order to register the feature in the database.
     * In PostgreSQL this is e.g. {@code CREATE EXTENSION IF NOT EXISTS <feature name>}.
     * Preferably the query should be <i>idempotent</i>.
     */
    String getFeatureRegistrationQuery();
}

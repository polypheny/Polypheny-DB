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

package org.polypheny.db.adapter.parquet.relational.filter;

import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.type.entity.PolyValue;

import java.util.List;

/**
 * A result returned by {@link ParquetSourceFileFilterReducer} containing information about evaluation and residual filters.
 *
 * @param matches evaluated filters were matched against the {@link org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile}.
 * @param filters remaining filters that still needs to be pushed down into the reading pipeline.
 */
public record ResidualFilters( boolean matches, List<ParquetAdapterFilter<PolyValue>> filters ) {

}

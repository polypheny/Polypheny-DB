/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.sql.language.validate;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.sql.language.SqlNode;


/**
 * Implementation of {@link SqlValidatorScope} that can see all schemas in the current catalog.
 *
 * Occurs near the root of the scope stack; its parent is typically {@link EmptyScope}.
 *
 * Helps resolve {@code schema.table.column} column references, such as
 *
 * <blockquote><pre>select sales.emp.empno from sales.emp</pre></blockquote>
 */
class CatalogScope extends DelegatingScope {

    /**
     * Fully-qualified name of the catalog. Typically empty or ["CATALOG"].
     */
    final ImmutableList<String> names;


    CatalogScope( SqlValidatorScope parent, List<String> names ) {
        super( parent );
        this.names = ImmutableList.copyOf( names );
    }


    @Override
    public SqlNode getNode() {
        throw new UnsupportedOperationException();
    }

}


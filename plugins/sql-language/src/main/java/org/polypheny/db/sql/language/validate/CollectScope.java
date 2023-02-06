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

package org.polypheny.db.sql.language.validate;


import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlNode;


/**
 * The name-resolution context for expression inside a multiset call. The objects visible are multiset expressions, and those inherited from the parent scope.
 *
 * @see CollectNamespace
 */
class CollectScope extends ListScope {

    private final SqlValidatorScope usingScope;
    private final SqlCall child;


    CollectScope( SqlValidatorScope parent, SqlValidatorScope usingScope, SqlCall child ) {
        super( parent );
        this.usingScope = usingScope;
        this.child = child;
    }


    @Override
    public SqlNode getNode() {
        return child;
    }

}

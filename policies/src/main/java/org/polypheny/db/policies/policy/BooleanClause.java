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

package org.polypheny.db.policies.policy;

import lombok.Getter;
import lombok.Setter;

public class BooleanClause extends Clause {

    @Getter
    @Setter
    private boolean value;


    public BooleanClause( ClauseName clauseName, boolean defaultValue, boolean isDefault, Category category, String description ) {
        super( clauseName, isDefault,  ClauseType.BOOLEAN, category, description );
        this.value = defaultValue;
    }


}

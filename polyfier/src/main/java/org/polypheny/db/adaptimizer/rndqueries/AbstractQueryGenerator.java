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

package org.polypheny.db.adaptimizer.rndqueries;

import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.Pair;

public abstract class AbstractQueryGenerator {

    @Getter
    protected final QueryTemplate template;

    protected AbstractQueryGenerator( QueryTemplate template ) {
        this.template = template;
    }

    public abstract Pair<AlgNode, Long> generate( Statement statement );

    public abstract long getLastSeed();

    public abstract void setSeed( long seed );

}

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

package org.polypheny.db.adapter.mongodb.rules;

import java.util.List;
import java.util.Map;
import lombok.NonNull;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.adapter.mongodb.MongoAlg;
import org.polypheny.db.adapter.mongodb.MongoEntity;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.document.DocumentModify;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;

public class MongoDocumentModify extends DocumentModify<MongoEntity> implements MongoAlg {


    protected MongoDocumentModify(
            AlgTraitSet traits,
            MongoEntity collection,
            AlgNode input,
            @NonNull Operation operation,
            Map<String, RexNode> updates,
            List<String> removes,
            Map<String, String> renames ) {
        super( traits, collection, input, operation, updates, removes, renames );
    }


    @Override
    public void implement( Implementor implementor ) {
        Implementor condImplementor = new Implementor( true );
        condImplementor.setStaticRowType( implementor.getStaticRowType() );
        ((MongoAlg) input).implement( condImplementor );

        throw new NotImplementedException();
    }

}

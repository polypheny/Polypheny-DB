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

package org.polypheny.db.algebra.polyalg.arguments;

import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.catalog.entity.Entity;

public class EntityArg implements PolyAlgArg {

    @Getter
    private final Entity entity;


    public EntityArg( Entity entity ) {
        this.entity = entity;
    }


    @Override
    public ParamType getType() {
        return ParamType.ENTITY;
    }


    @Override
    public String toPolyAlg( AlgNode context, @NonNull List<String> inputFieldNames ) {
        try {
            return entity.getNamespaceName() + "." + entity.name;
        } catch ( UnsupportedOperationException e ) {
            return entity.name + "." + entity.id;
        }
    }


}

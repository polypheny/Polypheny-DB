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

import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.PolyAlgUtils;

public class CollationArg implements PolyAlgArg {

    private final AlgFieldCollation arg;
    private final AlgNode algNode;


    public CollationArg( AlgFieldCollation arg, AlgNode fieldNameProvider ) {
        this.arg = arg;
        this.algNode = fieldNameProvider;
    }


    @Override
    public ParamType getType() {
        return ParamType.COLLATION;
    }


    @Override
    public String toPolyAlg() {
        String str = PolyAlgUtils.getFieldNameFromIndex( algNode, arg.getFieldIndex() );
        if ( arg.direction != AlgFieldCollation.Direction.ASCENDING || arg.nullDirection != arg.direction.defaultNullDirection() ) {
            str += " " + arg.shortString();
        }
        return str;
    }

}

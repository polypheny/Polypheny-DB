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
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;

public class CollationArg implements PolyAlgArg {

    public static final CollationArg NULL = new CollationArg( null );

    @Getter
    private final AlgFieldCollation coll;


    public CollationArg( AlgFieldCollation coll ) {
        this.coll = coll;
    }


    @Override
    public ParamType getType() {
        return ParamType.COLLATION;
    }


    @Override
    public String toPolyAlg( AlgNode context, @NonNull List<String> inputFieldNames ) {
        if ( coll == null ) {
            return "";
        }
        int idx = coll.getFieldIndex();
        String str = inputFieldNames.size() > idx ? inputFieldNames.get( idx ) : Integer.toString( idx );
        boolean notDefaultNullDir = coll.nullDirection != coll.direction.defaultNullDirection();
        if ( coll.direction != AlgFieldCollation.Direction.ASCENDING || notDefaultNullDir ) {
            str += " " + coll.direction.shortString;
            if ( notDefaultNullDir ) {
                str += " " + coll.nullDirection.toString();
            }
        }
        return str;
    }

}

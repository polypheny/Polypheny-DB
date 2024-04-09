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
import lombok.NonNull;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;

public class CollationArg implements PolyAlgArg {

    public static final CollationArg NULL = new CollationArg( null );

    private final AlgFieldCollation arg;


    public CollationArg( AlgFieldCollation arg ) {
        this.arg = arg;
    }


    @Override
    public ParamType getType() {
        return ParamType.COLLATION;
    }


    @Override
    public String toPolyAlg( AlgNode context, @NonNull List<String> inputFieldNames ) {
        if ( arg == null ) {
            return "";
        }
        int idx = arg.getFieldIndex();
        String str = inputFieldNames.size() > idx ? inputFieldNames.get( idx ) : Integer.toString( idx );
        if ( arg.direction != AlgFieldCollation.Direction.ASCENDING || arg.nullDirection != arg.direction.defaultNullDirection() ) {
            str += " " + arg.shortString();
        }
        return str;
    }

}

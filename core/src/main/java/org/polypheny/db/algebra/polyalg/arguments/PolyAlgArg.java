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
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;

public interface PolyAlgArg {

    ParamType getType();

    /**
     * Returns the PolyAlg representation of this argument.
     *
     * @param context the AlgNode this argument belongs to
     * @param inputFieldNames list of field names of all children with duplicate names uniquified
     * @return string representing the PolyAlg representation of this argument
     */
    String toPolyAlg( AlgNode context, @NonNull List<String> inputFieldNames );

}

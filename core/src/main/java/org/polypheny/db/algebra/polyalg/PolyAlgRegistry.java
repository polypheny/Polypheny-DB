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

package org.polypheny.db.algebra.polyalg;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.Map;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.logical.relational.*;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.Parameter;

public class PolyAlgRegistry {

    private static final Map<Class<? extends AlgNode>, PolyAlgDeclaration> declarations = new HashMap<>();


    static {
        declarations.put( LogicalRelProject.class, new PolyAlgDeclaration( "PROJECT", 1,
                new Parameter( "projects", ParamType.SIMPLE_REX, true ) ) );

        declarations.put( RelScan.class, new PolyAlgDeclaration( "SCAN", 0,
                new Parameter( "entity", ParamType.ENTITY ) ) );

        declarations.put( LogicalRelFilter.class, new PolyAlgDeclaration( "FILTER", 1, ImmutableList.of(
                new Parameter( "condition", ParamType.SIMPLE_REX, false ),
                new Parameter( "variables", ParamType.ANY, true, "" ) ) ) );

        declarations.put( LogicalRelAggregate.class, new PolyAlgDeclaration( "AGG", 1, ImmutableList.of(
                new Parameter( "group", ParamType.FIELD, true ),
                new Parameter( "groups", ParamType.ANY, true, "" ),
                new Parameter( "aggs", ParamType.AGGREGATE, true, "" ) ) ) );

        declarations.put( LogicalRelMinus.class, new PolyAlgDeclaration( "MINUS", 2, ImmutableList.of(
                new Parameter( "all", ParamType.BOOLEAN, false, "false" ) ) ) );

        declarations.put( LogicalRelSort.class, new PolyAlgDeclaration( "SORT", 1, ImmutableList.of(
                new Parameter( "collation", ParamType.COLLATION, false ),
                new Parameter( "fetch", ParamType.SIMPLE_REX, false, "" ),
                new Parameter( "offset", ParamType.SIMPLE_REX, false, "" ) ) ) );

        declarations.put( LogicalRelJoin.class, new PolyAlgDeclaration( "JOIN", 2, ImmutableList.of(
                new Parameter( "condition", ParamType.SIMPLE_REX, false ),
                new Parameter( "type", ParamType.ANY, false, "INNER" ),
                new Parameter( "variables", ParamType.CORR_ID, true, "" ),
                new Parameter( "semiJoinDone", ParamType.BOOLEAN, false, "FALSE" ),
                new Parameter( "sysFields", ParamType.ANY, true, "" ) ) ) );
    }


    public static PolyAlgDeclaration getDeclaration( Class<? extends AlgNode> clazz ) {
        return getDeclaration( clazz, 0 );
    }


    /**
     * Retrieves the PolyAlgDeclaration associated with the specified class from a map of declarations,
     * or returns a default PolyAlgDeclaration if none is found.
     *
     * @param clazz The class for which the PolyAlgDeclaration is being retrieved
     * @param numInputs The number of inputs associated with the PolyAlgDeclaration if a new one is created.
     * @return The PolyAlgDeclaration associated with the specified class if found in the map,
     * or a new PolyAlgDeclaration initialized with the class name and the specified number of inputs.
     */
    public static PolyAlgDeclaration getDeclaration( Class<? extends AlgNode> clazz, int numInputs ) {
        return declarations.getOrDefault(
                clazz,
                new PolyAlgDeclaration( clazz.getSimpleName(), numInputs ) );
    }

}

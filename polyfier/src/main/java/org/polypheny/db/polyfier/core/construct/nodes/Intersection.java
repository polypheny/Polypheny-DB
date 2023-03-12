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

package org.polypheny.db.polyfier.core.construct.nodes;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.util.Pair;

@Getter
@Setter(AccessLevel.PROTECTED)
public class Intersection extends SetNode {
    static int idx = 0;
    public static void resetIndex() {
        idx = 0;
    }

    protected Intersection() {
        super( idx++ );
    }

    public static Intersection intersection( Pair<Node, Node> nodes ) {
        Intersection intersection = new Intersection();
        intersection.setOperatorType( OperatorType.INTERSECT );
        return (Intersection) intersection.setPrep( nodes );
    }


}
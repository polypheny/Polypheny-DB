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
public class Union extends SetNode {
    private static int idx = 0;

    public static void resetIndex() {
        idx = 0;
    }

    protected Union() {
        super( idx++ );
    }

    public static Union union( Pair<Node, Node> nodes ) {
        Union union = new Union();
        union.setOperatorType( OperatorType.UNION );
        return (Union) union.setPrep( nodes );
    }


}

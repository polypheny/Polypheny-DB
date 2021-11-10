/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.core;

import java.util.Set;

public interface Node extends Cloneable {


    @Deprecated
    Object clone();

    /**
     * Creates a copy of a SqlNode.
     */
    static <E extends Node> E clone( E e ) {
        //noinspection unchecked
        return (E) e.clone( e.getPos() );
    }

    /**
     * Clones a SqlNode with a different position.
     */
    Node clone( ParserPos pos );

    Kind getKind();

    boolean isA( Set<Kind> category );

    String toString();

    ParserPos getPos();

}

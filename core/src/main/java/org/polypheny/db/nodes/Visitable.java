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

package org.polypheny.db.nodes;

public interface Visitable {

    /**
     * Accepts a generic visitor.
     *
     * Implementations of this method in subtypes simply call the appropriate <code>visit</code> method on the {@link NodeVisitor visitor object}.
     *
     * The type parameter <code>R</code> must be consistent with the type parameter of the visitor.
     */
    <R> R accept( NodeVisitor<R> visitor );

}

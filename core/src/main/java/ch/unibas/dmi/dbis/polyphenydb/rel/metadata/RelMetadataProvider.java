/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.rel.metadata;


import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import com.google.common.collect.Multimap;
import java.lang.reflect.Method;


/**
 * RelMetadataProvider defines an interface for obtaining metadata about relational expressions. This interface is weakly-typed and is not intended to be called directly in most contexts;
 * instead, use a strongly-typed facade such as {@link RelMetadataQuery}.
 *
 * For background and motivation, see <a href="http://wiki.eigenbase.org/RelationalExpressionMetadata">wiki</a>.
 *
 * If your provider is not a singleton, we recommend that you implement {@link Object#equals(Object)} and {@link Object#hashCode()} methods. This makes the cache of {@link JaninoRelMetadataProvider} more effective.
 */
public interface RelMetadataProvider {

    /**
     * Retrieves metadata of a particular type and for a particular sub-class of relational expression.
     *
     * The object returned is a function. It can be applied to a relational expression of the given type to create a metadata object.
     *
     * For example, you might call
     *
     * <blockquote><pre>
     * RelMetadataProvider provider;
     * LogicalFilter filter;
     * RexNode predicate;
     * Function&lt;RelNode, Metadata&gt; function = provider.apply(LogicalFilter.class, Selectivity.class};
     * Selectivity selectivity = function.apply(filter);
     * Double d = selectivity.selectivity(predicate);
     * </pre></blockquote>
     *
     * @param relClass Type of relational expression
     * @param metadataClass Type of metadata
     * @return Function that will field a metadata instance; or null if this provider cannot supply metadata of this type
     */
    <M extends Metadata> UnboundMetadata<M> apply( Class<? extends RelNode> relClass, Class<? extends M> metadataClass );

    <M extends Metadata> Multimap<Method, MetadataHandler<M>> handlers( MetadataDef<M> def );
}


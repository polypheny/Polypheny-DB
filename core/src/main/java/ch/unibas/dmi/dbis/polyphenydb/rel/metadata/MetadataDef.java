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
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;


/**
 * Definition of metadata.
 *
 * @param <M> Kind of metadata
 */
public class MetadataDef<M extends Metadata> {

    public final Class<M> metadataClass;
    public final Class<? extends MetadataHandler<M>> handlerClass;
    public final ImmutableList<Method> methods;


    private MetadataDef( Class<M> metadataClass, Class<? extends MetadataHandler<M>> handlerClass, Method... methods ) {
        this.metadataClass = metadataClass;
        this.handlerClass = handlerClass;
        this.methods = ImmutableList.copyOf( methods );
        final Method[] handlerMethods = handlerClass.getDeclaredMethods();

        // Handler must have the same methods as Metadata, each method having additional "subclass-of-RelNode, RelMetadataQuery" parameters.
        assert handlerMethods.length == methods.length;
        for ( Pair<Method, Method> pair : Pair.zip( methods, handlerMethods ) ) {
            final List<Class<?>> leftTypes = Arrays.asList( pair.left.getParameterTypes() );
            final List<Class<?>> rightTypes = Arrays.asList( pair.right.getParameterTypes() );
            assert leftTypes.size() + 2 == rightTypes.size();
            assert RelNode.class.isAssignableFrom( rightTypes.get( 0 ) );
            assert RelMetadataQuery.class == rightTypes.get( 1 );
            assert leftTypes.equals( rightTypes.subList( 2, rightTypes.size() ) );
        }
    }


    /**
     * Creates a {@link ch.unibas.dmi.dbis.polyphenydb.rel.metadata.MetadataDef}.
     */
    public static <M extends Metadata> MetadataDef<M> of( Class<M> metadataClass, Class<? extends MetadataHandler<M>> handlerClass, Method... methods ) {
        return new MetadataDef<>( metadataClass, handlerClass, methods );
    }
}


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

package ch.unibas.dmi.dbis.polyphenydb.plan;


import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionConfig;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/**
 * Utilities for {@link Context}.
 */
public class Contexts {

    public static final EmptyContext EMPTY_CONTEXT = new EmptyContext();


    private Contexts() {
    }


    /**
     * Returns a context that contains a {@link PolyphenyDbConnectionConfig}.
     *
     * @deprecated Use {@link #of}
     */
    @Deprecated // to be removed before 2.0
    public static Context withConfig( PolyphenyDbConnectionConfig config ) {
        return of( config );
    }


    /**
     * Returns a context that returns null for all inquiries.
     */
    public static Context empty() {
        return EMPTY_CONTEXT;
    }


    /**
     * Returns a context that wraps an object.
     *
     * A call to {@code unwrap(C)} will return {@code target} if it is an instance of {@code C}.
     */
    public static Context of( Object o ) {
        return new WrapContext( o );
    }


    /**
     * Returns a context that wraps an array of objects, ignoring any nulls.
     */
    public static Context of( Object... os ) {
        final List<Context> contexts = new ArrayList<>();
        for ( Object o : os ) {
            if ( o != null ) {
                contexts.add( of( o ) );
            }
        }
        return chain( contexts );
    }


    /**
     * Returns a context that wraps a list of contexts.
     *
     * A call to {@code unwrap(C)} will return the first object that is an instance of {@code C}.
     *
     * If any of the contexts is a {@link Context}, recursively looks in that object. Thus this method can be used to chain contexts.
     */
    public static Context chain( Context... contexts ) {
        return chain( ImmutableList.copyOf( contexts ) );
    }


    private static Context chain( Iterable<? extends Context> contexts ) {
        // Flatten any chain contexts in the list, and remove duplicates
        final List<Context> list = new ArrayList<>();
        for ( Context context : contexts ) {
            build( list, context );
        }
        switch ( list.size() ) {
            case 0:
                return empty();
            case 1:
                return list.get( 0 );
            default:
                return new ChainContext( ImmutableList.copyOf( list ) );
        }
    }


    /**
     * Recursively populates a list of contexts.
     */
    private static void build( List<Context> list, Context context ) {
        if ( context == EMPTY_CONTEXT || list.contains( context ) ) {
            return;
        }
        if ( context instanceof ChainContext ) {
            ChainContext chainContext = (ChainContext) context;
            for ( Context child : chainContext.contexts ) {
                build( list, child );
            }
        } else {
            list.add( context );
        }
    }


    /**
     * Context that wraps an object.
     */
    private static class WrapContext implements Context {

        final Object target;


        WrapContext( Object target ) {
            this.target = Objects.requireNonNull( target );
        }


        public <T> T unwrap( Class<T> clazz ) {
            if ( clazz.isInstance( target ) ) {
                return clazz.cast( target );
            }
            return null;
        }
    }


    /**
     * Empty context.
     */
    public static class EmptyContext implements Context {

        public <T> T unwrap( Class<T> clazz ) {
            return null;
        }
    }


    /**
     * Context that wraps a chain of contexts.
     */
    private static final class ChainContext implements Context {

        final ImmutableList<Context> contexts;


        ChainContext( ImmutableList<Context> contexts ) {
            this.contexts = Objects.requireNonNull( contexts );
            for ( Context context : contexts ) {
                assert !(context instanceof ChainContext) : "must be flat";
            }
        }


        @Override
        public <T> T unwrap( Class<T> clazz ) {
            for ( Context context : contexts ) {
                final T t = context.unwrap( clazz );
                if ( t != null ) {
                    return t;
                }
            }
            return null;
        }
    }
}

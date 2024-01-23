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

package org.polypheny.db.algebra;

import com.google.common.collect.ImmutableMap;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.calcite.linq4j.function.Function2;
import org.jetbrains.annotations.Nullable;

/**
 * @param <Return>
 */
public interface AlgProducingVisitor<Return> {

    default Return handle( AlgNode visitable ) {
        Function<AlgNode, Return> handler = findHandler( visitable.getClass() );
        if ( handler != null ) {
            return handler.apply( visitable );
        }

        return getDefaultHandler().apply( visitable );
    }

    default @Nullable Function<AlgNode, Return> findHandler( Class<?> clazz ) {
        while ( clazz != null && clazz != AlgNode.class ) {
            if ( getHandlers().containsKey( clazz ) ) {
                return getHandlers().get( clazz );
            }
            clazz = clazz.getSuperclass();
        }

        return null;
    }

    ImmutableMap<Class<? extends AlgNode>, Function<AlgNode, Return>> getHandlers();

    Function<AlgNode, Return> getDefaultHandler();

    interface AlgConsumingVisitor {

        default void handle( AlgNode visitable ) {
            Consumer<AlgNode> handler = findHandler( visitable.getClass() );
            if ( handler != null ) {
                handler.accept( visitable );
                return;
            }

            getDefaultHandler().accept( visitable );
        }


        default @Nullable Consumer<AlgNode> findHandler( Class<?> clazz ) {
            while ( clazz != null && clazz != AlgNode.class ) {
                if ( getHandlers().containsKey( clazz ) ) {
                    return getHandlers().get( clazz );
                }
                clazz = clazz.getSuperclass();
            }

            return null;
        }


        ImmutableMap<Class<? extends AlgNode>, Consumer<AlgNode>> getHandlers();


        Consumer<AlgNode> getDefaultHandler();

    }


    interface AlgProducingVisitor2<Return, Param1> {

        default Return handle( AlgNode visitable, Param1 param1 ) {
            Function2<AlgNode, Param1, Return> handler = findHandler( visitable.getClass() );
            if ( handler != null ) {
                return handler.apply( visitable, param1 );
            }

            return getDefaultHandler().apply( visitable, param1 );
        }


        default @Nullable Function2<AlgNode, Param1, Return> findHandler( Class<?> clazz ) {
            while ( clazz != null && clazz != AlgNode.class ) {
                if ( getHandlers().containsKey( clazz ) ) {
                    return getHandlers().get( clazz );
                }
                clazz = clazz.getSuperclass();
            }

            return null;
        }


        ImmutableMap<Class<? extends AlgNode>, Function2<AlgNode, Param1, Return>> getHandlers();


        Function2<AlgNode, Param1, Return> getDefaultHandler();

    }


    interface AlgProducingVisitor3<Return, Param1, Param2> {

        default Return handle( AlgNode visitable, Param1 param1, Param2 param2 ) {
            Function3<AlgNode, Param1, Param2, Return> handler = findHandler( visitable.getClass() );
            if ( handler != null ) {
                return handler.apply( visitable, param1, param2 );
            }

            return getDefaultHandler().apply( visitable, param1, param2 );
        }


        default @Nullable Function3<AlgNode, Param1, Param2, Return> findHandler( Class<?> clazz ) {
            while ( clazz != null && clazz != AlgNode.class ) {
                if ( getHandlers().containsKey( clazz ) ) {
                    return getHandlers().get( clazz );
                }
                clazz = clazz.getSuperclass();
            }

            return getDefaultHandler();
        }


        ImmutableMap<Class<? extends AlgNode>, Function3<AlgNode, Param1, Param2, Return>> getHandlers();


        Function3<AlgNode, Param1, Param2, Return> getDefaultHandler();

    }


    @FunctionalInterface
    interface Function3<Param1, Param2, Param3, Return> {


        Return apply( Param1 param1, Param2 param2, Param3 param3 );

    }

}

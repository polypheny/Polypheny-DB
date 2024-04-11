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

package org.polypheny.db.algebra.enumerable.impl;


import java.util.List;
import java.util.function.Function;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.enumerable.RexToLixTranslator;
import org.polypheny.db.algebra.enumerable.WinAggAddContext;
import org.polypheny.db.algebra.enumerable.WinAggFrameResultContext;
import org.polypheny.db.algebra.enumerable.WinAggImplementor;


/**
 * Implementation of {@link WinAggAddContext}.
 */
public abstract class WinAggAddContextImpl extends WinAggResultContextImpl implements WinAggAddContext {

    public WinAggAddContextImpl( BlockBuilder block, List<Expression> accumulator, Function<BlockBuilder, WinAggFrameResultContext> frame ) {
        super( block, accumulator, frame );
    }


    @Override
    public final RexToLixTranslator rowTranslator() {
        return rowTranslator( computeIndex( Expressions.constant( 0 ), WinAggImplementor.SeekType.AGG_INDEX ) );
    }


    @Override
    public final List<Expression> arguments() {
        return rowTranslator().translateList( rexArguments() );
    }
}

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

package org.polypheny.db.sql.language.validate;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.Util.FoundOne;


/**
 * Visitor that looks for an aggregate function inside a tree of {@link SqlNode} objects and throws {@link FoundOne} when it finds one.
 */
class AggFinder extends AggVisitor {

    /**
     * Creates an AggFinder.
     *
     * @param opTab Operator table
     * @param over Whether to find windowed function calls {@code agg(x) OVER windowSpec}
     * @param aggregate Whether to find non-windowed aggregate calls
     * @param group Whether to find group functions (e.g. {@code TUMBLE})
     * @param delegate Finder to which to delegate when processing the arguments
     */
    AggFinder( OperatorTable opTab, boolean over, boolean aggregate, boolean group, AggFinder delegate ) {
        super( opTab, over, aggregate, group, delegate );
    }


    /**
     * Finds an aggregate.
     *
     * @param node Parse tree to search
     * @return First aggregate function in parse tree, or null if not found
     */
    public SqlCall findAgg( SqlNode node ) {
        try {
            node.accept( this );
            return null;
        } catch ( FoundOne e ) {
            Util.swallow( e, null );
            return (SqlCall) e.getNode();
        }
    }


    public SqlCall findAgg( List<SqlNode> nodes ) {
        try {
            for ( SqlNode node : nodes ) {
                node.accept( this );
            }
            return null;
        } catch ( Util.FoundOne e ) {
            Util.swallow( e, null );
            return (SqlCall) e.getNode();
        }
    }


    @Override
    protected Void found( Call call ) {
        throw new Util.FoundOne( call );
    }


    /**
     * Creates a copy of this finder that has the same parameters as this, then returns the list of all aggregates found.
     */
    Iterable<SqlCall> findAll( Iterable<SqlNode> nodes ) {
        final AggIterable aggIterable = new AggIterable( opTab, over, aggregate, group, delegate );
        for ( SqlNode node : nodes ) {
            node.accept( aggIterable );
        }
        return aggIterable.calls;
    }


    /**
     * Iterates over all aggregates.
     */
    static class AggIterable extends AggVisitor implements Iterable<SqlCall> {

        private final List<SqlCall> calls = new ArrayList<>();


        AggIterable( OperatorTable opTab, boolean over, boolean aggregate, boolean group, AggFinder delegate ) {
            super( opTab, over, aggregate, group, delegate );
        }


        @Override
        protected Void found( Call call ) {
            calls.add( (SqlCall) call );
            return null;
        }


        @Override
        @Nonnull
        public Iterator<SqlCall> iterator() {
            return calls.iterator();
        }

    }

}


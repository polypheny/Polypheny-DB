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
import java.util.List;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Syntax;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.nodes.BasicNodeVisitor;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.fun.SqlAbstractGroupFunction;


/**
 * Visitor that can find aggregate and windowed aggregate functions.
 *
 * @see AggFinder
 */
abstract class AggVisitor extends BasicNodeVisitor<Void> {

    protected final OperatorTable opTab;

    /**
     * Whether to find windowed aggregates.
     */
    protected final boolean over;
    protected final AggFinder delegate;

    /**
     * Whether to find regular (non-windowed) aggregates.
     */
    protected boolean aggregate;

    /**
     * Whether to find group functions (e.g. {@code TUMBLE}) or group auxiliary functions (e.g. {@code TUMBLE_START}).
     */
    protected boolean group;


    /**
     * Creates an AggVisitor.
     *
     * @param opTab Operator table
     * @param over Whether to find windowed function calls {@code agg(x) OVER windowSpec}
     * @param aggregate Whether to find non-windowed aggregate calls
     * @param group Whether to find group functions (e.g. {@code TUMBLE})
     * @param delegate Finder to which to delegate when processing the arguments
     */
    AggVisitor( OperatorTable opTab, boolean over, boolean aggregate, boolean group, AggFinder delegate ) {
        this.group = group;
        this.over = over;
        this.aggregate = aggregate;
        this.delegate = delegate;
        this.opTab = opTab;
    }


    @Override
    public Void visit( Call call ) {
        final Operator operator = call.getOperator();
        // If nested aggregates disallowed or found an aggregate at invalid level
        if ( operator.isAggregator() && !(operator instanceof SqlAbstractGroupFunction) && !operator.requiresOver() ) {
            if ( delegate != null ) {
                return operator.acceptCall( delegate, call );
            }
            if ( aggregate ) {
                return found( call );
            }
        }
        if ( group && operator.isGroup() ) {
            return found( call );
        }
        // User-defined function may not be resolved yet.
        if ( operator instanceof SqlFunction ) {
            final SqlFunction sqlFunction = (SqlFunction) operator;
            if ( sqlFunction.getFunctionCategory().isUserDefinedNotSpecificFunction() ) {
                final List<Operator> list = new ArrayList<>();
                opTab.lookupOperatorOverloads( sqlFunction.getSqlIdentifier(), sqlFunction.getFunctionCategory(), Syntax.FUNCTION, list );
                for ( Operator operator2 : list ) {
                    if ( operator2.isAggregator() && !operator2.requiresOver() ) {
                        // If nested aggregates disallowed or found aggregate at invalid level
                        if ( aggregate ) {
                            found( call );
                        }
                    }
                }
            }
        }
        if ( call.isA( Kind.QUERY ) ) {
            // don't traverse into queries
            return null;
        }
        if ( call.getKind() == Kind.OVER ) {
            if ( over ) {
                return found( call );
            } else {
                // an aggregate function over a window is not an aggregate!
                return null;
            }
        }
        return super.visit( call );
    }


    protected abstract Void found( Call call );

}


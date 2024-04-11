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

package org.polypheny.db.adapter.ethereum;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.Pair;

public class EthereumPredicateFactory {

    static final List<Kind> REX_COMPARATORS = new ArrayList<>() {{
        this.add( Kind.EQUALS );
        this.add( Kind.LESS_THAN );
        this.add( Kind.LESS_THAN_OR_EQUAL );
        this.add( Kind.GREATER_THAN );
        this.add( Kind.GREATER_THAN_OR_EQUAL );
    }};
    static final Predicate<BigInteger> ALWAYS_TRUE = bigInteger -> true;


    static Predicate<BigInteger> makePredicate( DataContext dataContext, List<RexNode> filters, EthereumMapper mapper ) {
        String field = "$0";
        if ( mapper == EthereumMapper.TRANSACTION ) {
            field = "$3";
        }
        String blockNumberField = field;
        return bigInteger -> {
            boolean result = true;
            for ( RexNode filter : filters ) {
                Pair<Boolean, Boolean> intermediateResult = match( bigInteger, dataContext, filter, blockNumberField );
                if ( !intermediateResult.left ) {
                    continue;
                }
                result = intermediateResult.right;
                if ( !result ) {
                    break;
                }
            }
            return result;
        };
    }


    private static Pair<Boolean, Boolean> match( BigInteger bigInteger, DataContext dataContext, RexNode filter, String blockNumberField ) {
        boolean result = true;
        boolean exists = false;

        if ( filter.isA( REX_COMPARATORS ) ) {
            final RexCall call = (RexCall) filter;
            RexNode left = call.getOperands().get( 0 );
            if ( left.isA( Kind.CAST ) ) {
                left = ((RexCall) left).operands.get( 0 );
            }
            final RexNode right = call.getOperands().get( 1 );
            if ( left instanceof RexIndexRef && right instanceof RexDynamicParam ) {
                if ( !((RexIndexRef) left).getName().equals( blockNumberField ) ) // $0 is the in
                {
                    return new Pair<>( false, false );
                }
                exists = true;
                BigInteger value = new BigInteger( String.valueOf( dataContext.getParameterValue( ((RexDynamicParam) right).getIndex() ) ) );
                if ( filter.isA( Kind.EQUALS ) ) {
                    result = bigInteger.compareTo( value ) == 0;
                } else if ( filter.isA( Kind.LESS_THAN ) ) {
                    result = bigInteger.compareTo( value ) < 0;
                } else if ( filter.isA( Kind.LESS_THAN_OR_EQUAL ) ) {
                    result = bigInteger.compareTo( value ) <= 0;
                } else if ( filter.isA( Kind.GREATER_THAN ) ) {
                    result = bigInteger.compareTo( value ) > 0;
                } else if ( filter.isA( Kind.GREATER_THAN_OR_EQUAL ) ) {
                    result = bigInteger.compareTo( value ) >= 0;
                }
            }
        } else if ( filter.isA( Kind.AND ) ) {
            for ( RexNode and : ((RexCall) filter).getOperands() ) {
                Pair<Boolean, Boolean> x = match( bigInteger, dataContext, and, blockNumberField );
                exists |= x.left;
                if ( x.left ) {
                    result &= x.right;
                }
            }
        } else if ( filter.isA( Kind.OR ) ) {
            result = false;
            for ( RexNode and : ((RexCall) filter).getOperands() ) {
                Pair<Boolean, Boolean> x = match( bigInteger, dataContext, and, blockNumberField );
                exists |= x.left;
                if ( x.left ) {
                    result |= x.right;
                }
            }
        } else if ( filter.isA( Kind.NOT ) ) {
            Pair<Boolean, Boolean> x = match( bigInteger, dataContext, ((RexCall) filter).getOperands().get( 0 ), blockNumberField );
            if ( x.left ) {
                exists = true;
                result = !x.right;
            }
        }
        return new Pair<>( exists, result );
    }

}

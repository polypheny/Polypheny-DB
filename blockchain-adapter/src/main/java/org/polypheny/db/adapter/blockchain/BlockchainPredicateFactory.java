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

package org.polypheny.db.adapter.blockchain;

import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.util.Pair;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class BlockchainPredicateFactory {
    static final List<SqlKind> REX_COMPARATORS = new ArrayList<SqlKind>() {{
        this.add(SqlKind.EQUALS);
        this.add(SqlKind.LESS_THAN);
        this.add(SqlKind.LESS_THAN_OR_EQUAL);
        this.add(SqlKind.GREATER_THAN);
        this.add(SqlKind.GREATER_THAN_OR_EQUAL);
    }};
    static final Predicate<BigInteger> ALWAYS_TRUE = bigInteger -> true;

    static Predicate<BigInteger> makePredicate(DataContext dataContext, List<RexNode> filters, BlockchainMapper mapper) {
        String field = "$0";
        if (mapper == BlockchainMapper.TRANSACTION) {
            field = "$3";
        }
        String blockNumberField = field;
        return bigInteger -> {
            boolean result = true;
            for (RexNode filter : filters) {
                Pair<Boolean, Boolean> intermediateResult = match(bigInteger, dataContext, filter, blockNumberField);
                if (!intermediateResult.left)
                    continue;
                result &= intermediateResult.right;
                if (!result)
                    break;
            }
            return result;
        };
    }

    private static Pair<Boolean, Boolean> match(BigInteger bigInteger, DataContext dataContext, RexNode filter, String blockNumberField) {
        boolean result = true;
        boolean exists = false;

        if (filter.isA(REX_COMPARATORS)) {
            final RexCall call = (RexCall) filter;
            RexNode left = call.getOperands().get(0);
            if (left.isA(SqlKind.CAST)) {
                left = ((RexCall) left).operands.get(0);
            }
            final RexNode right = call.getOperands().get(1);
            if (left instanceof RexInputRef && right instanceof RexDynamicParam) {
                if (!((RexInputRef) left).getName().equals(blockNumberField)) // $0 is the in
                    return new Pair<>(false, false);
                exists = true;
                BigInteger value = new BigInteger(String.valueOf(dataContext.getParameterValue(((RexDynamicParam) right).getIndex())));
                if (filter.isA(SqlKind.EQUALS)) {
                    result = bigInteger.compareTo(value) == 0;
                } else if (filter.isA(SqlKind.LESS_THAN)) {
                    result = bigInteger.compareTo(value) == -1;
                } else if (filter.isA(SqlKind.LESS_THAN_OR_EQUAL)) {
                    result = bigInteger.compareTo(value) <= 0;
                } else if (filter.isA(SqlKind.GREATER_THAN)) {
                    result = bigInteger.compareTo(value) == 1;
                } else if (filter.isA(SqlKind.GREATER_THAN_OR_EQUAL)) {
                    result = bigInteger.compareTo(value) >= 0;
                }
            }
        } else if (filter.isA(SqlKind.AND)) {
            for (RexNode and : ((RexCall) filter).getOperands()) {
                Pair<Boolean, Boolean> x = match(bigInteger, dataContext, and, blockNumberField);
                exists |= x.left;
                if (x.left) {
                    result &= x.right;
                }
            }
        } else if (filter.isA(SqlKind.OR)) {
            result = false;
            for (RexNode and : ((RexCall) filter).getOperands()) {
                Pair<Boolean, Boolean> x = match(bigInteger, dataContext, and, blockNumberField);
                exists |= x.left;
                if (x.left) {
                    result |= x.right;
                }
            }
        } else if (filter.isA(SqlKind.NOT)) {
            Pair<Boolean, Boolean> x = match(bigInteger, dataContext, ((RexCall) filter).getOperands().get(0), blockNumberField);
            if (x.left) {
                exists = true;
                result = !x.right;
            }
        }
        return new Pair<>(exists, result);
    }
}

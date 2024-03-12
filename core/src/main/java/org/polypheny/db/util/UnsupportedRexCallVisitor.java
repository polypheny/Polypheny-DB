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

package org.polypheny.db.util;

import java.util.List;
import java.util.function.Function;
import lombok.Getter;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexVisitorImpl;

public class UnsupportedRexCallVisitor extends RexVisitorImpl<Void> {

    private final List<Function<RexCall, Boolean>> unsupportedChecks;
    @Getter
    boolean containsUnsupportedRexCall = false;


    protected UnsupportedRexCallVisitor( List<Function<RexCall, Boolean>> unsupportedChecks ) {
        super( true );
        this.unsupportedChecks = unsupportedChecks;
    }


    @Override
    public Void visitCall( RexCall call ) {
        if ( unsupportedChecks.stream().anyMatch( c -> c.apply( call ) ) ) {
            containsUnsupportedRexCall = true;
        }
        return super.visitCall( call );
    }


    public static boolean containsArrayConstructor( List<RexNode> nodes ) {
        return nodes.stream().anyMatch( n -> containsUnsupportedCall( n, List.of( c -> c.op.getOperatorName() == OperatorName.ARRAY_VALUE_CONSTRUCTOR ) ) );
    }


    public static boolean containsArrayConstructorOrModelItem( List<RexNode> nodes ) {
        return nodes.stream().anyMatch( n -> containsUnsupportedCall( n,
                List.of( c -> c.op.getOperatorName() == OperatorName.ARRAY_VALUE_CONSTRUCTOR
                        || c.op.getOperatorName() == OperatorName.CROSS_MODEL_ITEM ) ) );
    }


    public static boolean containsModelItem( List<? extends RexNode> nodes ) {
        return nodes.stream().anyMatch( n -> containsUnsupportedCall( n,
                List.of( c -> c.op.getOperatorName() == OperatorName.CROSS_MODEL_ITEM ) ) );
    }


    public static boolean containsUnsupportedCall( List<RexNode> nodes, List<Function<RexCall, Boolean>> unsupportedChecks ) {
        return nodes.stream().anyMatch( n -> containsUnsupportedCall( n, unsupportedChecks ) );
    }


    public static boolean containsUnsupportedCall( RexNode node, List<Function<RexCall, Boolean>> unsupportedChecks ) {
        UnsupportedRexCallVisitor visitor = new UnsupportedRexCallVisitor( unsupportedChecks );
        node.accept( visitor );
        return visitor.containsUnsupportedRexCall;
    }

}

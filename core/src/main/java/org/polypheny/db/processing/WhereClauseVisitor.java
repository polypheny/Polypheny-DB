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

package org.polypheny.db.processing;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Where clause visitor which identifies partitions.
 */
public class WhereClauseVisitor extends RexShuttle {

    private final Statement statement;
    @Getter
    private final List<PolyValue> values = new ArrayList<>();
    private final long partitionColumnIndex;
    @Getter
    protected boolean valueIdentified = false;
    @Getter
    private boolean unsupportedFilter = false;


    public WhereClauseVisitor( Statement statement, long partitionColumnIndex ) {
        super();
        this.statement = statement;
        this.partitionColumnIndex = partitionColumnIndex;
    }


    @Override
    public RexNode visitCall( final RexCall call ) {
        super.visitCall( call );

        if ( call.operands.size() == 2 ) {
            if ( call.op.getKind() == Kind.EQUALS ) {
                PolyValue value;
                if ( call.operands.get( 0 ) instanceof RexIndexRef ) {
                    if ( ((RexIndexRef) call.operands.get( 0 )).getIndex() == partitionColumnIndex ) {
                        if ( call.operands.get( 1 ) instanceof RexLiteral ) {
                            value = ((RexLiteral) call.operands.get( 1 )).value;
                            values.add( value );
                            valueIdentified = true;
                        } else if ( call.operands.get( 1 ) instanceof RexDynamicParam ) {
                            long index = ((RexDynamicParam) call.operands.get( 1 )).getIndex();
                            value = statement.getDataContext().getParameterValue( index );//.get("?" + index);
                            values.add( value );
                            valueIdentified = true;
                        }
                    }
                } else if ( call.operands.get( 1 ) instanceof RexIndexRef ) {
                    if ( ((RexIndexRef) call.operands.get( 1 )).getIndex() == partitionColumnIndex ) {
                        if ( call.operands.get( 0 ) instanceof RexLiteral ) {
                            value = ((RexLiteral) call.operands.get( 0 )).getValue();
                            values.add( value );
                            valueIdentified = true;
                        } else if ( call.operands.get( 0 ) instanceof RexDynamicParam ) {
                            long index = ((RexDynamicParam) call.operands.get( 0 )).getIndex();
                            value = statement.getDataContext().getParameterValue( index );//get("?" + index); //.getParameterValues //
                            values.add( value );
                            valueIdentified = true;
                        }
                    }
                }
            } else {
                unsupportedFilter = true;
            }
        }
        return call;
    }

}

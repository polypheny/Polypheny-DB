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

package org.polypheny.db.adapter.file.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.file.FileAlg.FileImplementor;
import org.polypheny.db.adapter.file.Value;
import org.polypheny.db.adapter.file.Value.DynamicValue;
import org.polypheny.db.adapter.file.Value.InputValue;
import org.polypheny.db.adapter.file.Value.LiteralValue;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyValue;

public class FileUtil {

    public static List<Value> getUpdates( final List<RexNode> exps, FileImplementor implementor ) {
        List<Value> valueList = new ArrayList<>();
        int offset;
        boolean noCheck;
        if ( exps.size() == implementor.getFileTable().columns.size() ) {
            noCheck = true;
            offset = 0;
        } else {
            noCheck = false;
            offset = implementor.getFileTable().columns.size();
        }
        for ( int i = offset; i < implementor.getFileTable().columns.size() + offset; i++ ) {
            if ( noCheck || exps.size() > i ) {
                RexNode lit = exps.get( i );
                if ( lit instanceof RexLiteral literal ) {
                    valueList.add( new LiteralValue( null, literal.value ) );
                } else if ( lit instanceof RexDynamicParam dynamicParam ) {
                    valueList.add( new DynamicValue( null, dynamicParam.getIndex() ) );
                } else if ( lit instanceof RexIndexRef indexRef ) {
                    valueList.add( new InputValue( indexRef.getIndex(), indexRef.getIndex() ) );
                } else if ( lit instanceof RexCall call && lit.getType().getPolyType() == PolyType.ARRAY ) {
                    valueList.add( fromArrayRexCall( call ) );
                } else {
                    throw new GenericRuntimeException( "Could not implement " + lit.getClass().getSimpleName() + " " + lit );
                }
            }
        }
        return valueList;
    }


    public static Value fromArrayRexCall( final RexCall call ) {
        List<PolyValue> arrayValues = new ArrayList<>();
        for ( RexNode node : call.getOperands() ) {
            arrayValues.add( ((RexLiteral) node).value );
        }
        return new LiteralValue( null, PolyList.of( arrayValues ) );
    }


    public static Expression getValuesExpression( final List<Value> values ) {
        List<Expression> valueConstructors = new ArrayList<>();
        for ( Value value : values ) {
            valueConstructors.add( value.asExpression() );
        }
        return EnumUtils.constantArrayList( valueConstructors, PolyValue.class );
    }

}

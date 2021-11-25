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

package org.polypheny.db.languages.rex;


import java.util.HashMap;
import java.util.Map;
import org.polypheny.db.core.nodes.Operator;
import org.polypheny.db.rex.RexCall;


/**
 * Implementation of {@link RexSqlConvertletTable}.
 */
public class RexSqlReflectiveConvertletTable implements RexSqlConvertletTable {

    private final Map<Object, Object> map = new HashMap<>();


    public RexSqlReflectiveConvertletTable() {
    }


    @Override
    public RexSqlConvertlet get( RexCall call ) {
        RexSqlConvertlet convertlet;
        final Operator op = call.getOperator();

        // Is there a convertlet for this operator (e.g. SqlStdOperatorTable.plusOperator)?
        convertlet = (RexSqlConvertlet) map.get( op );
        if ( convertlet != null ) {
            return convertlet;
        }

        // Is there a convertlet for this class of operator (e.g. SqlBinaryOperator)?
        Class<?> clazz = op.getClass();
        while ( clazz != null ) {
            convertlet = (RexSqlConvertlet) map.get( clazz );
            if ( convertlet != null ) {
                return convertlet;
            }
            clazz = clazz.getSuperclass();
        }

        // Is there a convertlet for this class of expression (e.g. SqlCall)?
        clazz = call.getClass();
        while ( clazz != null ) {
            convertlet = (RexSqlConvertlet) map.get( clazz );
            if ( convertlet != null ) {
                return convertlet;
            }
            clazz = clazz.getSuperclass();
        }
        return null;
    }


    /**
     * Registers a convertlet for a given operator instance
     *
     * @param op Operator instance, say {@link org.polypheny.db.core.StdOperatorRegistry #MINUS}
     * @param convertlet Convertlet
     */
    protected void registerOp( Operator op, RexSqlConvertlet convertlet ) {
        map.put( op, convertlet );
    }

}


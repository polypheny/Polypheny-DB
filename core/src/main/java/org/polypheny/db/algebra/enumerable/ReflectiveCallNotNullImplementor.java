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

package org.polypheny.db.algebra.enumerable;


import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.rex.RexCall;


/**
 * Implementation of {@link NotNullImplementor} that calls a given {@link java.lang.reflect.Method}.
 * <p>
 * When method is not static, a new instance of the required class is created.
 */
public class ReflectiveCallNotNullImplementor implements NotNullImplementor {

    protected final Method method;


    /**
     * Constructor of {@link ReflectiveCallNotNullImplementor}
     *
     * @param method method that is used to implement the call
     */
    public ReflectiveCallNotNullImplementor( Method method ) {
        this.method = method;
    }


    @Override
    public Expression implement( RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands ) {
        translatedOperands = EnumUtils.fromInternal( method.getParameterTypes(), translatedOperands );
        if ( (method.getModifiers() & Modifier.STATIC) != 0 ) {
            return Expressions.call( method, translatedOperands );
        } else {
            // The UDF class must have a public zero-args constructor. Assume that the validator checked already.
            final Expression target = Expressions.new_( method.getDeclaringClass() );
            return Expressions.call( target, method, translatedOperands );
        }
    }

}

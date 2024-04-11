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

package org.polypheny.db.sql.sql2alg;


import com.google.common.base.Preconditions;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlOperator;


/**
 * Implementation of {@link SqlRexConvertletTable} which uses reflection to call any method of the form <code>public RexNode convertXxx(ConvertletContext, SqlNode)</code> or
 * <code>public RexNode convertXxx(ConvertletContext, SqlOperator, SqlCall)</code>.
 */
public class ReflectiveConvertletTable implements SqlRexConvertletTable {

    private final Map<Object, Object> map = new HashMap<>();


    public ReflectiveConvertletTable() {
        for ( final Method method : getClass().getMethods() ) {
            registerNodeTypeMethod( method );
            registerOpTypeMethod( method );
        }
    }


    /**
     * Registers method if it: a. is public, and b. is named "convertXxx", and c. has a return type of "RexNode" or a subtype d. has a 2 parameters with
     * types ConvertletContext and SqlNode (or a subtype) respectively.
     */
    private void registerNodeTypeMethod( final Method method ) {
        if ( !Modifier.isPublic( method.getModifiers() ) ) {
            return;
        }
        if ( !method.getName().startsWith( "convert" ) ) {
            return;
        }
        if ( !RexNode.class.isAssignableFrom( method.getReturnType() ) ) {
            return;
        }
        final Class<?>[] parameterTypes = method.getParameterTypes();
        if ( parameterTypes.length != 2 ) {
            return;
        }
        if ( parameterTypes[0] != SqlRexContext.class ) {
            return;
        }
        final Class<?> parameterType = parameterTypes[1];
        if ( !SqlNode.class.isAssignableFrom( parameterType ) ) {
            return;
        }
        map.put( parameterType, (SqlRexConvertlet) ( cx, call ) -> {
            try {
                return (RexNode) method.invoke( ReflectiveConvertletTable.this, cx, call );
            } catch ( IllegalAccessException | InvocationTargetException e ) {
                throw new GenericRuntimeException( "while converting " + call, e );
            }
        } );
    }


    /**
     * Registers method if it: a. is public, and b. is named "convertXxx", and c. has a return type of "RexNode" or a subtype d. has a 3 parameters with
     * types: ConvertletContext; SqlOperator (or a subtype), SqlCall (or a subtype).
     */
    private void registerOpTypeMethod( final Method method ) {
        if ( !Modifier.isPublic( method.getModifiers() ) ) {
            return;
        }
        if ( !method.getName().startsWith( "convert" ) ) {
            return;
        }
        if ( !RexNode.class.isAssignableFrom( method.getReturnType() ) ) {
            return;
        }
        final Class<?>[] parameterTypes = method.getParameterTypes();
        if ( parameterTypes.length != 3 ) {
            return;
        }
        if ( parameterTypes[0] != SqlRexContext.class ) {
            return;
        }
        final Class<?> opClass = parameterTypes[1];
        if ( !SqlOperator.class.isAssignableFrom( opClass ) ) {
            return;
        }
        final Class<?> parameterType = parameterTypes[2];
        if ( !SqlCall.class.isAssignableFrom( parameterType ) ) {
            return;
        }
        map.put( opClass, (SqlRexConvertlet) ( cx, call ) -> {
            try {
                return (RexNode) method.invoke( ReflectiveConvertletTable.this, cx, call.getOperator(), call );
            } catch ( IllegalAccessException | InvocationTargetException e ) {
                throw new GenericRuntimeException( "while converting " + call, e );
            }
        } );
    }


    @Override
    public SqlRexConvertlet get( SqlCall call ) {
        SqlRexConvertlet convertlet;
        final Operator op = call.getOperator();

        // Is there a convertlet for this operator (e.g. SqlStdOperatorTable.plusOperator)?
        convertlet = (SqlRexConvertlet) map.get( op );
        if ( convertlet != null ) {
            return convertlet;
        }

        // Is there a convertlet for this class of operator (e.g. SqlBinaryOperator)?
        Class<?> clazz = op.getClass();
        while ( clazz != null ) {
            convertlet = (SqlRexConvertlet) map.get( clazz );
            if ( convertlet != null ) {
                return convertlet;
            }
            clazz = clazz.getSuperclass();
        }

        // Is there a convertlet for this class of expression (e.g. SqlCall)?
        clazz = call.getClass();
        while ( clazz != null ) {
            convertlet = (SqlRexConvertlet) map.get( clazz );
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
     * @param op Operator instance, say {@link OperatorRegistry #MINUS}
     * @param convertlet Convertlet
     */
    protected void registerOp( Operator op, SqlRexConvertlet convertlet ) {
        map.put( op, convertlet );
    }


    /**
     * Registers that one operator is an alias for another.
     *
     * @param alias Operator which is alias
     * @param target Operator to translate calls to
     */
    protected void addAlias( final Operator alias, final Operator target ) {
        map.put(
                alias, (SqlRexConvertlet) ( cx, call ) -> {
                    Preconditions.checkArgument(
                            call.getOperator().equals( alias ),
                            "call to wrong operator" );
                    final SqlCall newCall = (SqlCall) target.createCall( ParserPos.ZERO, call.getOperandList() );
                    return cx.convertExpression( newCall );
                } );
    }

}

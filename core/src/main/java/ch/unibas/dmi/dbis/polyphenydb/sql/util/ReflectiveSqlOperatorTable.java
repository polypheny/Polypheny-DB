/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.sql.util;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSyntax;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Locale;


/**
 * ReflectiveSqlOperatorTable implements the {@link SqlOperatorTable} interface by reflecting the public fields of a subclass.
 */
public abstract class ReflectiveSqlOperatorTable implements SqlOperatorTable {

    public static final String IS_NAME = "INFORMATION_SCHEMA";

    private final Multimap<Key, SqlOperator> operators = HashMultimap.create();


    protected ReflectiveSqlOperatorTable() {
    }


    /**
     * Performs post-constructor initialization of an operator table. It can't be part of the constructor, because the subclass constructor needs to complete first.
     */
    public final void init() {
        // Use reflection to register the expressions stored in public fields.
        for ( Field field : getClass().getFields() ) {
            try {
                if ( SqlFunction.class.isAssignableFrom( field.getType() ) ) {
                    SqlFunction op = (SqlFunction) field.get( this );
                    if ( op != null ) {
                        register( op );
                    }
                } else if ( SqlOperator.class.isAssignableFrom( field.getType() ) ) {
                    SqlOperator op = (SqlOperator) field.get( this );
                    register( op );
                }
            } catch ( IllegalArgumentException | IllegalAccessException e ) {
                Util.throwIfUnchecked( e.getCause() );
                throw new RuntimeException( e.getCause() );
            }
        }
    }


    // implement SqlOperatorTable
    @Override
    public void lookupOperatorOverloads( SqlIdentifier opName, SqlFunctionCategory category, SqlSyntax syntax, List<SqlOperator> operatorList ) {
        // NOTE jvs 3-Mar-2005:  ignore category until someone cares

        String simpleName;
        if ( opName.names.size() > 1 ) {
            if ( opName.names.get( opName.names.size() - 2 ).equals( IS_NAME ) ) {
                // per SQL99 Part 2 Section 10.4 Syntax Rule 7.b.ii.1
                simpleName = Util.last( opName.names );
            } else {
                return;
            }
        } else {
            simpleName = opName.getSimple();
        }

        // Always look up built-in operators case-insensitively. Even in sessions with unquotedCasing=UNCHANGED and caseSensitive=true.
        final Collection<SqlOperator> list = operators.get( new Key( simpleName, syntax ) );
        if ( list.isEmpty() ) {
            return;
        }
        for ( SqlOperator op : list ) {
            if ( op.getSyntax() == syntax ) {
                operatorList.add( op );
            } else if ( syntax == SqlSyntax.FUNCTION && op instanceof SqlFunction ) {
                // this special case is needed for operators like CAST, which are treated as functions but have special syntax
                operatorList.add( op );
            }
        }

        // REVIEW jvs: Why is this extra lookup required? Shouldn't it be covered by search above?
        switch ( syntax ) {
            case BINARY:
            case PREFIX:
            case POSTFIX:
                for ( SqlOperator extra : operators.get( new Key( simpleName, syntax ) ) ) {
                    // REVIEW: Should only search operators added during this method?
                    if ( extra != null && !operatorList.contains( extra ) ) {
                        operatorList.add( extra );
                    }
                }
                break;
        }
    }


    /**
     * Registers a function or operator in the table.
     */
    public void register( SqlOperator op ) {
        operators.put( new Key( op.getName(), op.getSyntax() ), op );
    }


    @Override
    public List<SqlOperator> getOperatorList() {
        return ImmutableList.copyOf( operators.values() );
    }


    /**
     * Key for looking up operators. The name is stored in upper-case because we store case-insensitively, even in a case-sensitive session.
     */
    private static class Key extends Pair<String, SqlSyntax> {

        Key( String name, SqlSyntax syntax ) {
            super( name.toUpperCase( Locale.ROOT ), normalize( syntax ) );
        }


        private static SqlSyntax normalize( SqlSyntax syntax ) {
            switch ( syntax ) {
                case BINARY:
                case PREFIX:
                case POSTFIX:
                    return syntax;
                default:
                    return SqlSyntax.FUNCTION;
            }
        }
    }
}


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

package org.polypheny.db.sql.language.util;


import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Syntax;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Function;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * ReflectiveSqlOperatorTable implements the {@link OperatorTable} interface by reflecting the public fields of a subclass.
 */
public abstract class ReflectiveSqlOperatorTable implements OperatorTable {

    public static final String IS_NAME = "INFORMATION_SCHEMA";

    private final Multimap<Key, Operator> operators = HashMultimap.create();


    protected ReflectiveSqlOperatorTable() {
    }


    /**
     * Performs post-constructor initialization of an operator table. It can't be part of the constructor, because the subclass constructor needs to complete first.
     */
    public final void init() {
        OperatorRegistry
                .getAllOperators()
                .forEach( ( name, operator ) -> register( operator ) );
    }


    // implement SqlOperatorTable
    @Override
    public void lookupOperatorOverloads( Identifier opName, FunctionCategory category, Syntax syntax, List<Operator> operatorList ) {
        // NOTE jvs 3-Mar-2005:  ignore category until someone cares

        String simpleName;
        if ( opName.getNames().size() > 1 ) {
            if ( opName.getNames().get( opName.getNames().size() - 2 ).equals( IS_NAME ) ) {
                // per SQL99 Part 2 Section 10.4 Syntax Rule 7.b.ii.1
                simpleName = Util.last( opName.getNames() );
            } else {
                return;
            }
        } else {
            simpleName = opName.getSimple();
        }

        // Always look up built-in operators case-insensitively. Even in sessions with unquotedCasing=UNCHANGED and caseSensitive=true.
        final Collection<Operator> list = operators.get( new Key( simpleName, syntax ) );
        if ( list.isEmpty() ) {
            return;
        }
        for ( Operator op : list ) {
            if ( op.getSyntax() == syntax ) {
                operatorList.add( op );
            } else if ( syntax == Syntax.FUNCTION && op instanceof Function ) {
                // this special case is needed for operators like CAST, which are treated as functions but have special syntax
                operatorList.add( op );
            }
        }

        // REVIEW jvs: Why is this extra lookup required? Shouldn't it be covered by search above?
        switch ( syntax ) {
            case BINARY:
            case PREFIX:
            case POSTFIX:
                for ( Operator extra : operators.get( new Key( simpleName, syntax ) ) ) {
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
    public void register( Operator op ) {
        operators.put( new Key( op.getName(), op.getSyntax() ), op );
    }


    @Override
    public List<Operator> getOperatorList() {
        return ImmutableList.copyOf( operators.values() );
    }


    /**
     * Key for looking up operators. The name is stored in upper-case because we store case-insensitively, even in a case-sensitive session.
     */
    private static class Key extends Pair<String, Syntax> {

        Key( String name, Syntax syntax ) {
            super( name.toUpperCase( Locale.ROOT ), normalize( syntax ) );
        }


        private static Syntax normalize( Syntax syntax ) {
            return switch ( syntax ) {
                case BINARY, PREFIX, POSTFIX -> syntax;
                default -> Syntax.FUNCTION;
            };
        }

    }

}


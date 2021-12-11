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

package org.polypheny.db.algebra.operators;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Syntax;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Operator;


/**
 * ChainedSqlOperatorTable implements the {@link OperatorTable} interface by chaining together any number of underlying operator table instances.
 */
public class ChainedOperatorTable implements OperatorTable {

    protected final List<OperatorTable> tableList;


    /**
     * Creates a table based on a given list.
     */
    public ChainedOperatorTable( List<OperatorTable> tableList ) {
        this.tableList = ImmutableList.copyOf( tableList );
    }


    /**
     * Creates a {@code ChainedSqlOperatorTable}.
     */
    public static OperatorTable of( OperatorTable... tables ) {
        return new ChainedOperatorTable( ImmutableList.copyOf( tables ) );
    }


    /**
     * Adds an underlying table. The order in which tables are added is significant; tables added earlier have higher lookup precedence. A table is not added if it is already on the list.
     *
     * @param table table to add
     */
    public void add( OperatorTable table ) {
        if ( !tableList.contains( table ) ) {
            tableList.add( table );
        }
    }


    @Override
    public void lookupOperatorOverloads( Identifier opName, FunctionCategory category, Syntax syntax, List<Operator> operatorList ) {
        for ( OperatorTable table : tableList ) {
            table.lookupOperatorOverloads( opName, category, syntax, operatorList );
        }
    }


    @Override
    public List<Operator> getOperatorList() {
        List<Operator> list = new ArrayList<>();
        for ( OperatorTable table : tableList ) {
            list.addAll( table.getOperatorList() );
        }
        return list;
    }

}


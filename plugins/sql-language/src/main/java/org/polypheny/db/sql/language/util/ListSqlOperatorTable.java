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


import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Syntax;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlOperator;


/**
 * Implementation of the {@link OperatorTable} interface by using a list of {@link SqlOperator operators}.
 */
public class ListSqlOperatorTable implements OperatorTable {

    @Getter
    private final List<SqlOperator> operatorList;


    public ListSqlOperatorTable() {
        this( new ArrayList<>() );
    }


    public ListSqlOperatorTable( List<SqlOperator> operatorList ) {
        this.operatorList = operatorList;
    }


    public void add( SqlOperator op ) {
        operatorList.add( op );
    }


    @Override
    public void lookupOperatorOverloads( Identifier opName, FunctionCategory category, Syntax syntax, List<Operator> operatorList ) {
        for ( SqlOperator operator : this.operatorList ) {
            if ( operator.getSyntax() != syntax ) {
                continue;
            }
            if ( !opName.isSimple() || !operator.isName( opName.getSimple() ) ) {
                continue;
            }
            if ( category != null && category != category( operator ) && !category.isUserDefinedNotSpecificFunction() ) {
                continue;
            }
            operatorList.add( operator );
        }
    }


    protected static FunctionCategory category( SqlOperator operator ) {
        if ( operator instanceof SqlFunction ) {
            return ((SqlFunction) operator).getFunctionCategory();
        } else {
            return FunctionCategory.SYSTEM;
        }
    }

}

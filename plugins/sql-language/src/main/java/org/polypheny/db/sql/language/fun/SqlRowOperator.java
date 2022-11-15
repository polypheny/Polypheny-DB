/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.sql.language.fun;


import java.util.AbstractList;
import java.util.Map;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.nodes.RowOperator;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.sql.language.SqlSyntax;
import org.polypheny.db.sql.language.SqlUtil;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.InferTypes;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.Pair;


/**
 * SqlRowOperator represents the special ROW constructor.
 *
 * TODO: describe usage for row-value construction and row-type construction (SQL supports both).
 */
public class SqlRowOperator extends SqlSpecialOperator implements RowOperator {


    public SqlRowOperator( String name ) {
        super(
                name,
                Kind.ROW, MDX_PRECEDENCE,
                false,
                null,
                InferTypes.RETURN_TYPE,
                OperandTypes.VARIADIC );
        assert name.equals( "ROW" ) || name.equals( " " );
    }


    // implement SqlOperator
    @Override
    public SqlSyntax getSqlSyntax() {
        // Function syntax would work too.
        return SqlSyntax.SPECIAL;
    }


    @Override
    public AlgDataType inferReturnType( final OperatorBinding opBinding ) {
        // The type of a ROW(e1,e2) expression is a record with the types {e1type,e2type}.  According to the standard, field names are implementation-defined.
        return opBinding.getTypeFactory().createStructType(
                new AbstractList<Map.Entry<String, AlgDataType>>() {
                    @Override
                    public Map.Entry<String, AlgDataType> get( int index ) {
                        return Pair.of(
                                CoreUtil.deriveAliasFromOrdinal( index ),
                                opBinding.getOperandType( index ) );
                    }


                    @Override
                    public int size() {
                        return opBinding.getOperandCount();
                    }
                } );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        SqlUtil.unparseFunctionSyntax( this, writer, call );
    }


    // override SqlOperator
    @Override
    public boolean requiresDecimalExpansion() {
        return false;
    }

}


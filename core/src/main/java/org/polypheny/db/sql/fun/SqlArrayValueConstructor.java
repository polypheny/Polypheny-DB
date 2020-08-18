/*
 * Copyright 2019-2020 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.sql.fun;


import com.google.gson.Gson;
import java.lang.reflect.Type;
import java.util.Objects;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.SqlBasicCall;
import org.polypheny.db.sql.SqlCall;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlOperatorBinding;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.validate.SqlValidator;
import org.polypheny.db.sql.validate.SqlValidatorScope;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;


/**
 * Definition of the SQL:2003 standard ARRAY constructor, <code>ARRAY[&lt;expr&gt;, ...]</code>.
 */
public class SqlArrayValueConstructor extends SqlMultisetValueConstructor {

    private static final Gson gson = new Gson();

    public final int dimension;
    public final int maxCardinality;
    public boolean outermost = true;

    public SqlArrayValueConstructor() {
        this( 0, 0, 0 );
    }


    /**
     * Constructor
     * @param dimension The dimension of this array. The most nested array has dimension 1, its parent has dimension 2 etc.
     * @param cardinality The cardinality of the array
     * @param maxCardinality If an array consists of nested arrays that have a larger cardinality, this value will be larger than the array's <i>cardinality</i>.
     */
    public SqlArrayValueConstructor ( final int dimension, final int cardinality, final int maxCardinality ) {
        super( "ARRAY", SqlKind.ARRAY_VALUE_CONSTRUCTOR );
        this.dimension = dimension;
        this.maxCardinality = Math.max( maxCardinality, cardinality );
    }


    @Override
    public RelDataType inferReturnType( SqlOperatorBinding opBinding ) {
        RelDataType type = getComponentType( opBinding.getTypeFactory(), opBinding.collectOperandTypes() );
        if ( null == type ) {
            return null;
        }
        return PolyTypeUtil.createArrayType( opBinding.getTypeFactory(), type, false, dimension, maxCardinality );
    }

    @Override
    public RelDataType deriveType( SqlValidator validator, SqlValidatorScope scope, SqlCall call ) {
        RelDataType type = super.deriveType( validator, scope, call );
        if( type instanceof ArrayType ) {
            ((ArrayType) type).setCardinality( maxCardinality ).setDimension( dimension );
        }
        //set the operator again, because SqlOperator.deriveType will clear the dimension & cardinality of this constructor
        ((SqlBasicCall) call).setOperator( this );
        return type;
    }

    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        if(!writer.getDialect().supportsNestedArrays()) {

            if( outermost ) {
                writer.literal( "'" );
            }

            final SqlWriter.Frame frame = writer.startList( "[", "]" );
            for ( SqlNode operand : call.getOperandList() ) {
                writer.sep( "," );
                operand.unparse( writer, leftPrec, rightPrec );
            }
            writer.endList( frame );

            if( outermost ) {
                writer.literal( "'" );
            }
        } else {
            super.unparse( writer, call, leftPrec, rightPrec );
        }
    }

    @Override
    public int hashCode() {
        // FIXME js(knn): This is a hotfix to make the whole array creation function work properly in
        //   things like `function(ARRAY[...])`.
        //   It probably is not a good idea to not have dimension, cardinality, and max cardinality in
        //   the hash, but right now it works and gets me moving forwards.
        return Objects.hash( kind, "ARRAY" );
    }


    public static Object reparse( PolyType innerType, Long dimension, String stringValue ) {
        Type conversionType = PolyTypeUtil.createNestedListType( dimension, innerType );
        return gson.fromJson( stringValue.trim(), conversionType );
    }
}

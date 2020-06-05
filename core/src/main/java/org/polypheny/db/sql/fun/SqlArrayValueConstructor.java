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
import org.polypheny.db.type.PolyTypeUtil;


/**
 * Definition of the SQL:2003 standard ARRAY constructor, <code>ARRAY[&lt;expr&gt;, ...]</code>.
 */
public class SqlArrayValueConstructor extends SqlMultisetValueConstructor {

    public final int dimension;
    public final int cardinality;

    public SqlArrayValueConstructor() {
        this( 0, 0 );
    }


    public SqlArrayValueConstructor ( final int dimension, final int cardinality ) {
        super( "ARRAY", SqlKind.ARRAY_VALUE_CONSTRUCTOR );
        this.dimension = dimension;
        this.cardinality = cardinality;
    }


    @Override
    public RelDataType inferReturnType( SqlOperatorBinding opBinding ) {
        RelDataType type = getComponentType( opBinding.getTypeFactory(), opBinding.collectOperandTypes() );
        if ( null == type ) {
            return null;
        }
        return PolyTypeUtil.createArrayType( opBinding.getTypeFactory(), type, false, dimension, cardinality );
    }

    @Override
    public RelDataType deriveType( SqlValidator validator, SqlValidatorScope scope, SqlCall call ) {
        RelDataType type = super.deriveType( validator, scope, call );
        if( type instanceof ArrayType ) {
            ((ArrayType) type).setCardinality( cardinality ).setDimension( dimension );
        }
        //set the operator again, because SqlOperator.deriveType will clear the dimension & cardinality of this constructor
        ((SqlBasicCall) call).setOperator( this );
        return type;
    }

    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        if(!writer.getDialect().supportsNestedArrays()) {

            if( dimension == 1 ) {
                writer.literal( "'" );
            }

            final SqlWriter.Frame frame = writer.startList( "[", "]" );
            for ( SqlNode operand : call.getOperandList() ) {
                writer.sep( "," );
                operand.unparse( writer, leftPrec, rightPrec );
            }
            writer.endList( frame );

            if( dimension == 1 ) {
                writer.literal( "'" );
            }
        } else {
            super.unparse( writer, call, leftPrec, rightPrec );
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash( kind, "ARRAY", dimension, cardinality );
    }
}

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

package org.polypheny.db.sql.language.fun;


import com.google.gson.Gson;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.nodes.ArrayValueConstructor;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.sql.language.SqlBasicCall;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;


/**
 * Definition of the SQL:2003 standard ARRAY constructor, <code>ARRAY[&lt;expr&gt;, ...]</code>.
 */
public class SqlArrayValueConstructor extends SqlMultisetValueConstructor implements ArrayValueConstructor {

    private static final Gson gson = new Gson();

    public final int dimension;
    public final int maxCardinality;
    public boolean outermost = true;


    public SqlArrayValueConstructor() {
        this( 0, 0, 0 );
    }


    /**
     * Constructor
     *
     * @param dimension The dimension of this array. The most nested array has dimension 1, its parent has dimension 2 etc.
     * @param cardinality The cardinality of the array
     * @param maxCardinality If an array consists of nested arrays that have a larger cardinality, this value will be larger than the array's <i>cardinality</i>.
     */
    public SqlArrayValueConstructor( final int dimension, final int cardinality, final int maxCardinality ) {
        super( "ARRAY", Kind.ARRAY_VALUE_CONSTRUCTOR );
        this.dimension = dimension;
        this.maxCardinality = Math.max( maxCardinality, cardinality );
    }


    @Override
    public AlgDataType inferReturnType( OperatorBinding opBinding ) {
        AlgDataType type = getComponentType( opBinding.getTypeFactory(), opBinding.collectOperandTypes() );
        if ( null == type ) {
            return null;
        }
        return PolyTypeUtil.createArrayType( opBinding.getTypeFactory(), type, false, dimension, maxCardinality );
    }


    @Override
    public AlgDataType deriveType( Validator validator, ValidatorScope scope, Call call ) {
        AlgDataType type = super.deriveType( validator, scope, call );
        if ( type instanceof ArrayType ) {
            ((ArrayType) type).setCardinality( maxCardinality ).setDimension( dimension );
        }
        //set the operator again, because SqlOperator.deriveType will clear the dimension & cardinality of this constructor
        ((SqlBasicCall) call).setOperator( this );
        return type;
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        if ( !writer.getDialect().supportsNestedArrays() ) {
            List<Object> list = createListForArrays( call.getSqlOperandList() );
            writer.literal( "'" + gson.toJson( list ) + "'" );
        } else {
            super.unparse( writer, call, leftPrec, rightPrec );
        }
    }


    private List<Object> createListForArrays( List<SqlNode> operands ) {
        List<Object> list = new ArrayList<>( operands.size() );
        for ( SqlNode node : operands ) {
            if ( node instanceof SqlCall && ((SqlCall) node).getOperator().getKind() == Kind.CAST ) {
                // CAST(value AS type) -> value
                node = ((SqlCall) node).operand( 0 );
            }
            if ( node instanceof SqlLiteral ) {
                Object value = switch ( ((SqlLiteral) node).getTypeName() ) {
                    case CHAR, VARCHAR -> ((SqlLiteral) node).toValue();
                    case BOOLEAN -> ((SqlLiteral) node).booleanValue();
                    case DECIMAL -> ((SqlLiteral) node).bigDecimalValue();
                    case BIGINT -> ((SqlLiteral) node).value.asNumber().longValue();
                    default -> ((SqlLiteral) node).getValue();
                };
                list.add( value );
            } else if ( node instanceof SqlCall ) {
                list.add( createListForArrays( ((SqlCall) node).getSqlOperandList() ) );
            } else {
                throw new GenericRuntimeException( "Invalid array" );
            }
        }
        return list;
    }


    @Override
    public int hashCode() {
        return Objects.hash( kind, "ARRAY" );
    }


    public static Object reparse( PolyType innerType, Long dimension, String stringValue ) {
        Type conversionType = PolyTypeUtil.createNestedListType( dimension, innerType );
        if ( stringValue == null ) {
            return null;
        }
        return gson.fromJson( stringValue.trim(), conversionType );
    }

}

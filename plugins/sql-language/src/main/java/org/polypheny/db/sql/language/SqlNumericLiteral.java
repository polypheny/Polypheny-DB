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

package org.polypheny.db.sql.language;


import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.NumericLiteral;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.util.Util;


/**
 * A numeric SQL literal.
 */
public class SqlNumericLiteral extends SqlLiteral implements NumericLiteral {

    private Integer prec;
    private Integer scale;
    private boolean isExact;


    protected SqlNumericLiteral( PolyNumber value, Integer prec, Integer scale, boolean isExact, ParserPos pos ) {
        super( value,
                isExact ? PolyType.DECIMAL : PolyType.DOUBLE,
                pos );
        this.prec = prec;
        this.scale = scale;
        this.isExact = isExact;
    }


    public Integer getPrec() {
        return prec;
    }


    public Integer getScale() {
        return scale;
    }


    public boolean isExact() {
        return isExact;
    }


    @Override
    public SqlNumericLiteral clone( ParserPos pos ) {
        return new SqlNumericLiteral( (PolyNumber) value, getPrec(), getScale(), isExact, pos );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.literal( toValue() );
    }


    @Override
    public String toValue() {
        PolyBigDecimal bd = (PolyBigDecimal) value;
        if ( isExact ) {
            return ((PolyBigDecimal) value).value.toString();
        }
        return Util.toScientificNotation( bd.value );
    }


    @Override
    public AlgDataType createSqlType( AlgDataTypeFactory typeFactory ) {
        if ( isExact ) {
            int scaleValue = scale;
            if ( 0 == scaleValue ) {
                PolyBigDecimal bd = (PolyBigDecimal) value;
                PolyType result;
                long l = bd.longValue();
                if ( (l >= Integer.MIN_VALUE) && (l <= Integer.MAX_VALUE) ) {
                    result = PolyType.INTEGER;
                } else {
                    result = PolyType.BIGINT;
                }
                return typeFactory.createPolyType( result );
            }

            // else we have a decimal
            return typeFactory.createPolyType( PolyType.DECIMAL, prec, scaleValue );
        }

        // else we have a a float, real or double.  make them all double for now.
        return typeFactory.createPolyType( PolyType.DOUBLE );
    }


    public boolean isInteger() {
        return 0 == scale;
    }

}


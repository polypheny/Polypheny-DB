/*
 * Copyright 2019-2026 The Polypheny Project
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


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.util.Litmus;


/**
 * Internal operator for converting a mapped field. The operands are the source value and the target field name for diagnostics.
 * The mapping supplies the result type, including its nullability.
 * New expressions also include the mapping name and qualified target table name.
 */
public final class SqlMappingConvertOperator extends SqlSpecialOperator {

    private static final OperandCountRange OPERAND_COUNT = PolyOperandCountRanges.of( 4 );


    public SqlMappingConvertOperator() {
        super( "$mappingConvert", Kind.MAPPING_CONVERT );
    }


    @Override
    public OperandCountRange getOperandCountRange() {
        return OPERAND_COUNT;
    }


    @Override
    public boolean validRexOperands( int count, Litmus litmus ) {
        return litmus.check( OPERAND_COUNT.isValidCount( count ), "$mappingConvert requires a source value, target field name, mapping name and qualified target table name" );
    }

}

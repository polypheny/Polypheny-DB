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

package org.polypheny.db.catalog;


import java.math.BigDecimal;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ColumnStrategy;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.InitializerContext;
import org.polypheny.db.util.NullInitializerExpressionFactory;


/**
 * Default values for the "EMPDEFAULTS" table.
 */
class EmpInitializerExpressionFactory extends NullInitializerExpressionFactory {

    @Override
    public ColumnStrategy generationStrategy( AlgOptTable table, int iColumn ) {
        switch ( iColumn ) {
            case 0:
            case 1:
            case 5:
                return ColumnStrategy.DEFAULT;
            default:
                return super.generationStrategy( table, iColumn );
        }
    }


    @Override
    public RexNode newColumnDefaultValue( AlgOptTable table, int iColumn, InitializerContext context ) {
        final RexBuilder rexBuilder = context.getRexBuilder();
        final AlgDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        switch ( iColumn ) {
            case 0:
                return rexBuilder.makeExactLiteral( new BigDecimal( 123 ), typeFactory.createPolyType( PolyType.INTEGER ) );
            case 1:
                return rexBuilder.makeLiteral( "Bob" );
            case 5:
                return rexBuilder.makeExactLiteral( new BigDecimal( 555 ), typeFactory.createPolyType( PolyType.INTEGER ) );
            default:
                return rexBuilder.constantNull();
        }
    }

}


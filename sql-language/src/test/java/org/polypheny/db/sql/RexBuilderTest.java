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

package org.polypheny.db.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;

public class RexBuilderTest {

    @BeforeClass
    public static void init() {
        if ( !SqlRegisterer.isInit() ) {
            SqlRegisterer.registerOperators();
        }
    }


    /**
     * Test RexBuilder.ensureType()
     */
    @Test
    public void testEnsureTypeWithDifference() {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        RexBuilder builder = new RexBuilder( typeFactory );

        RexNode node = new RexLiteral( Boolean.TRUE, typeFactory.createPolyType( PolyType.BOOLEAN ), PolyType.BOOLEAN );
        RexNode ensuredNode = builder.ensureType( typeFactory.createPolyType( PolyType.INTEGER ), node, true );

        assertNotEquals( node, ensuredNode );
        assertEquals( ensuredNode.getType(), typeFactory.createPolyType( PolyType.INTEGER ) );
    }

}

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

package org.polypheny.db.sql.fun;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.functions.Functions;
import org.polypheny.db.type.entity.PolyFloatList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DistanceFunctionTest {

    @BeforeAll
    static void init() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    @SuppressWarnings( "unchecked" )
    @Test
    public void cosineMetricFloatArrayTest() {
        float[] a = { 1.0f, 0.0f };
        float[] b = { 0.0f, 1.0f };
        List<PolyNumber> al = (List<PolyNumber>) (List<?>) new PolyFloatList<PolyFloat>( a );
        List<PolyNumber> bl = (List<PolyNumber>) (List<?>) new PolyFloatList<PolyFloat>( b );
        PolyDouble result = Functions.distance( al, bl, PolyString.of( "COSINE" ) );
        assertEquals( 1.0, result.doubleValue(), 1e-6 );
    }


    @SuppressWarnings( "unchecked" )
    @Test
    public void l2MetricFloatArrayTest() {
        float[] a = { 3.0f, 0.0f };
        float[] b = { 0.0f, 4.0f };
        List<PolyNumber> al = (List<PolyNumber>) (List<?>) new PolyFloatList<PolyFloat>( a );
        List<PolyNumber> bl = (List<PolyNumber>) (List<?>) new PolyFloatList<PolyFloat>( b );
        PolyDouble result = Functions.distance( al, bl, PolyString.of( "L2" ) );
        assertEquals( 5.0, result.doubleValue(), 1e-6 );
    }


    @SuppressWarnings( "unchecked" )
    @Test
    public void l1MetricFloatArrayTest() {
        float[] a = { 3.0f, 4.0f };
        float[] b = { 0.0f, 0.0f };
        List<PolyNumber> al = (List<PolyNumber>) (List<?>) new PolyFloatList<PolyFloat>( a );
        List<PolyNumber> bl = (List<PolyNumber>) (List<?>) new PolyFloatList<PolyFloat>( b );
        PolyDouble result = Functions.distance( al, bl, PolyString.of( "L1" ) );
        assertEquals( 7.0, result.doubleValue(), 1e-6 );
    }
}

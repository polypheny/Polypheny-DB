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

package org.polypheny.db.policies.policy.selfadaptiveness;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class WeightedList<T> extends HashMap<T, Double> {


    public WeightedList() {
        super();
    }


    public static WeightedList<Object> avg( List<WeightedList<?>> list ) {
        List<Object> tempList = new ArrayList<>();
        WeightedList<Object> weightedList = new WeightedList<>();

        list.get( 0 ).forEach( ( k, v ) -> tempList.add( k) );
        for ( int i = 0; i < tempList.size(); i++ ) {
            double avg = 0L;
            for ( WeightedList<?> value : list ) {
                if ( value.containsKey( tempList.get( i )) ) {
                    avg += value.get( tempList.get( i ) );
                }
            }
            weightedList.put( tempList.get( i ), avg / list.size() );
        }
        return weightedList;
    }



    public static List<Object> weightedToList(WeightedList<Object> list){
        WeightedList<Object> orderedList = new WeightedList<>();
        list.entrySet().stream().sorted( Map.Entry.comparingByValue( Comparator.reverseOrder()) ).forEachOrdered( x -> orderedList.put(x.getKey(), x.getValue()));

        List<Object> withoutWeight = new ArrayList<>();
        orderedList.forEach( (k, v) -> withoutWeight.add( k ) );

        return withoutWeight;
    }



}

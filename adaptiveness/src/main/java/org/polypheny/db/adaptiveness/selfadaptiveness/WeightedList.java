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

package org.polypheny.db.adaptiveness.selfadaptiveness;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.util.Pair;


public class WeightedList<T> extends HashMap<T, Double> {


    public WeightedList() {
        super();
    }


    public static <T> WeightedList<T> avg( List<WeightedList<?>> list ) {
        List<Object> possibilities = new ArrayList<>();
        WeightedList<T> weightedList = new WeightedList<>();

        list.get( 0 ).forEach( ( k, v ) -> possibilities.add( k ) );
        for ( Object possibility : possibilities ) {
            double avg = 0L;
            for ( WeightedList<?> value : list ) {
                if ( value.containsKey( possibility ) ) {
                    avg += value.get( possibility );
                }
            }
            weightedList.put( (T) possibility, avg / list.size() );
        }
        return weightedList;
    }


    public static <T> List<T> weightedToList( WeightedList<T> list ) {
        Map<T, Double> orderedList = new LinkedHashMap<>();
        ((WeightedList<Object>) list).entrySet().stream().sorted( Map.Entry.comparingByValue( Comparator.reverseOrder() ) ).forEachOrdered( x -> orderedList.put( (T) x.getKey(), x.getValue() ) );

        List<T> withoutWeight = new ArrayList<>();
        orderedList.forEach( ( k, v ) -> withoutWeight.add( (T) k ) );

        return withoutWeight;
    }


    public static <T> WeightedList<T> listToWeighted( List<Object> possibilities ) {

        WeightedList<T> weightedList = new WeightedList<>();

        for ( Object possibility : possibilities ) {
            weightedList.put( (T) possibility, 0D );
        }

        return weightedList;

    }


    public static Pair<Double, Double> compareOverall( WeightedList<?> oldWeightedList, WeightedList<?> newWeightedList ) {
        double oldOverall = oldWeightedList.values().stream().mapToDouble( Double::doubleValue ).sum() / oldWeightedList.size();
        double newOverall = newWeightedList.values().stream().mapToDouble( Double::doubleValue ).sum() / newWeightedList.size();

        return new Pair<>( oldOverall, newOverall );
    }


    public static <T> Pair<Object, Object> comparefirst( WeightedList<?> oldWeightedList, WeightedList<?> newWeightedList ) {
        Object oldBestValue = oldWeightedList.entrySet().stream().max( Comparator.comparing( Map.Entry::getValue ) ).get().getKey();
        Object newBestValue = newWeightedList.entrySet().stream().max( Comparator.comparing( Map.Entry::getValue ) ).get().getKey();

        return new Pair<>( oldBestValue, newBestValue );

    }


    public static <T> T getBest( WeightedList<?> weightedList ) {
        return (T) weightedList.entrySet().stream().max( Comparator.comparing( Entry::getValue ) ).get().getKey();
    }

}

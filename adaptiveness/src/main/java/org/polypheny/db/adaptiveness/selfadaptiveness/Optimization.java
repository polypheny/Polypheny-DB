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

import java.util.HashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adaptiveness.policy.PoliceUtil.ClauseName;
import org.polypheny.db.adaptiveness.selfadaptiveness.SelfAdaptivAgent.InformationContext;

@Getter
public enum Optimization {

    SELECT_STORE(
            "store",
            "Rank the Stores to find the best to use",
            new HashMap<>() {{
                put( ClauseName.LANGUAGE_OPTIMIZATION, (( c ) -> {
                    WeightedList<Object> list = new WeightedList<>();
                    list.putAll( c.getPossibilities().stream().collect( Collectors.toMap( e -> e, e -> {
                        if ( ((DataStore) e).getAdapterDefault().getPreferredSchemaType() == c.getNameSpaceModel() ) {
                            return 1.0;
                        } else {
                            return -1.0;
                        }
                    } ) ) );
                    return list;
                }) );

            }}
    ),

    WORKLOAD_COMPLEX_QUERY( "workloadComplexQuery",
            "Rank options form workload trigger",
            new HashMap<>() {{
                put( ClauseName.SPEED_OPTIMIZATION, (( c ) -> {
                    WeightedList<Object> list = new WeightedList<>();
                    list.putAll( c.getPossibilities().stream().collect( Collectors.toMap( e -> e, e -> {
                        if ( ((DataStore) e).getAdapterDefault().getPreferredSchemaType() == c.getNameSpaceModel() ) {
                            return 1.0;
                        } else {
                            return -1.0;
                        }
                    } ) ) );
                    return list;
                }) );
                //put(ClauseName.SPACE_OPTIMIZATION, )

            }} );


    private final String key;
    private final String description;
    private final HashMap<ClauseName, Function<InformationContext, WeightedList<?>>> rank;


    Optimization( final String key, final String description, HashMap<ClauseName, Function<InformationContext, WeightedList<?>>> rank ) {
        this.key = key;
        this.description = description;
        this.rank = rank;

    }




    /*

    select best store
      different for different self adaptive options
      * space 0
      * speed 1  (does not really matter)
      * redundancy 1 (does not really matter)
      * language 2 (correct store depending on the query language)
        - includes
            select the best fit of store during make decision process depending on regular policies and self adapting options






    other possible rules:

        migrate data to another store
         + speed, redundancy, language
         - space
         includes:
            depolying of store that is in line with policy
            copy data to that store





     */
}

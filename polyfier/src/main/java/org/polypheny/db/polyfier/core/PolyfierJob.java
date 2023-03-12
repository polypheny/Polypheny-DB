/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.polyfier.core;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.*;
import java.util.stream.LongStream;

@Builder
@Setter
@Getter
public class PolyfierJob implements Serializable {

    SchemaConfig schemaConfig;
    DataConfig dataConfig;
    QueryConfig queryConfig;
    StoreConfig storeConfig;
    PartitionConfig partitionConfig;
    SeedsConfig seedsConfig;

    @AllArgsConstructor
    @Getter
    public static class SchemaConfig implements Serializable {
        String schema;
    }

    @AllArgsConstructor
    @Getter
    public static class DataConfig implements Serializable {
        HashMap<String, String> parameters;
    }

    @AllArgsConstructor
    @Getter
    public static class QueryConfig implements Serializable {
        HashMap<String, String> parameters;
        HashMap<String, Double> weights;
        int complexity;
    }

    @AllArgsConstructor
    @Getter
    public static class StoreConfig implements Serializable {
        HashMap<String, String> parameters;
        HashMap<String, String> stores;
    }

    @AllArgsConstructor
    @Getter
    public static class PartitionConfig implements Serializable {
        HashMap<String, String> parameters;
    }

    @AllArgsConstructor
    @Getter
    public static class SeedsConfig implements Serializable {
       List<String> ranges;

       public Iterator<Long> iterator() {
           Set<Long> seeds = new HashSet<>();
           ranges.forEach( range -> {
               String[] arr = range.split("-");
               LongStream.range( Long.parseLong( arr[0] ), Long.parseLong( arr[1] ) )
                       .boxed().forEach( seeds::add );
           });
           return seeds.iterator();
       }

    }


}

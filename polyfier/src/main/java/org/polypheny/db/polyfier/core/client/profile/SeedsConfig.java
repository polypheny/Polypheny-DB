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

package org.polypheny.db.polyfier.core.client.profile;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.Builder;

import java.io.Serializable;
import java.util.*;
import java.util.stream.LongStream;

@Slf4j
public class SeedsConfig extends Config implements Serializable {


    private final List<String> ranges;

    private SeedsConfig( List<String> ranges ) {
        this.ranges = ranges;
    }

    public static class SeedsBuilder implements Builder<SeedsConfig> {

        private final transient Set<Long> seeds;

        public boolean isEmpty() {
            return this.seeds.isEmpty();
        }

        public SeedsBuilder() {
            this.seeds = new HashSet<>();
        }

        public SeedsBuilder addSeeds( List<Long> seeds ) {
            this.seeds.addAll( seeds );
            return this;
        }

        public SeedsBuilder addRange( long lo, long hi ) {
            LongStream.range( lo, hi ).boxed().forEach( this.seeds::add );
            return this;
        }

        public SeedsBuilder removeSeeds( List<Long> seeds ) {
            seeds.forEach( this.seeds::remove );
            return this;
        }

        public SeedsBuilder removeRange( long lo, long hi ) {
            LongStream.range( lo, hi ).boxed().forEach( this.seeds::remove );
            return this;
        }

        public SeedsBuilder removeSeed( long seed ) {
            this.seeds.remove( seed );
            return this;
        }

        public SeedsBuilder addRanges( List<String> ranges ) {
            ranges.forEach( range -> {
                String[] arr = range.split("-");
                LongStream.range( Long.parseLong( arr[0] ), Long.parseLong( arr[1] ) )
                        .boxed().forEach( seeds::add );
            });
            return this;
        }


        @Override
        public SeedsConfig build() {
            List<String> ranges = new LinkedList<>();

            Iterator<Long> iter = this.seeds.stream().sorted().iterator();

            Long first = null;
            Long last = null;
            while ( iter.hasNext() ) {
                Long seed = iter.next();
                if ( first == null ) {
                    first = seed;
                } else {
                    if ( ! ( last + 1 == seed ) && iter.hasNext() ) {
                        ranges.add( String.format("%s-%s", first, last + 1 ) );
                        first = seed;
                    } else if ( ! iter.hasNext() ) {
                        ranges.add( String.format("%s-%s", first, last + 2 ) );
                    }
                }
                last = seed;
            }

            return new SeedsConfig( ranges );

        }

        public Iterator<Long> iter() {
            return this.seeds.iterator();
        }

    }

    public SeedsBuilder asBuilder() {
        SeedsBuilder seedsBuilder = new SeedsBuilder();
        seedsBuilder.addRanges( this.ranges );
        return seedsBuilder;
    }

    public Iterator<Long> iter() {
        return this.asBuilder().iter();
    }


}

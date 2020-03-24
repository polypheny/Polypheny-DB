/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.information;


import com.google.gson.JsonObject;
import com.google.gson.JsonSerializer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;
import org.apache.commons.lang3.time.StopWatch;


public class InformationDuration extends Information {

    /**
     * Duration in NanoSeconds
     */
    long duration = 0L;
    private HashMap<String, Duration> children = new HashMap<>();
    private boolean isChild = false;

    /**
     * Constructor
     *
     * @param group Group to which this InformationDuration belongs to
     */
    public InformationDuration( final InformationGroup group ) {
        super( UUID.randomUUID().toString(), group.getId() );
    }

    public Duration start( final String name ) {
        Duration d = new Duration( name );
        this.children.put( name, d );
        return d;
    }


    public void stop( final String name ) {
        this.duration += this.children.get( name ).stop();
    }

    public Duration get( final String name ) {
        return this.children.get( name );
    }

    public Duration addNanoDuration( final String name, final long nanoDuration ) {
        Duration d = new Duration( name, nanoDuration );
        this.children.put( name, d );
        this.duration += nanoDuration;
        return d;
    }

    public Duration addMilliDuration( final String name, final long milliDuration ) {
        Duration d = new Duration( name, milliDuration * 1_000_000L );
        this.children.put( name, d );
        this.duration += milliDuration * 1_000_000L;
        return d;
    }

    static JsonSerializer<InformationDuration> getSerializer() {
        JsonSerializer<InformationDuration> serializer = ( src, typeOfSrc, context ) -> {
            JsonObject jsonObj = new JsonObject();
            jsonObj.addProperty( "type", src.type );
            jsonObj.add( "duration", context.serialize( src.duration ) );
            Object[] children = src.children.values().toArray();
            Arrays.sort( children );
            jsonObj.add( "children", context.serialize( children ) );
            jsonObj.add( "isChild", context.serialize( src.isChild ) );
            return jsonObj;
        };
        return serializer;
    }

    /**
     * Helper class for Durations
     */
    static class Duration implements Comparable<Duration> {

        private String type = InformationDuration.class.getSimpleName();//for the UI
        private String name;
        /**
         * Duration in NanoSeconds
         */
        private long duration;
        /**
         * If the duration is longer than the limit, the UI will indicate.
         */
        private long limit;
        private StopWatch sw;
        private long sequence;

        private boolean noProgressBar = false;
        static long counter = 0;

        private HashMap<String, Duration> children = new HashMap<>();
        private boolean isChild = true;

        private Duration( final String name ) {
            this.sequence = counter++;
            this.name = name;
            this.sw = StopWatch.createStarted();
        }

        private Duration( final String name, final long nanoDuration ) {
            this.sequence = counter++;
            this.name = name;
            this.duration = nanoDuration;
        }

        public long stop() {
            this.sw.stop();
            long time = this.sw.getNanoTime();
            this.duration = time;
            return time;
        }

        public long stop( final String childName ) {
            return this.children.get( childName ).stop();
        }

        public Duration start( final String name ) {
            Duration d = new Duration( name );
            this.children.put( name, d );
            return d;
        }

        public Duration get( final String name ) {
            return this.children.get( name );
        }

        /**
         * Set the limit in milliseconds. If the task too more time than the limit, it will be marked in the UI
         *
         * @param milliSeconds limit in milliseconds
         */
        public Duration setLimit( final long milliSeconds ) {
            this.limit = milliSeconds * 1_000_000;
            return this;
        }

        /**
         * Hide the progressbar for this Duration's children
         */
        public Duration noProgressBar() {
            this.noProgressBar = true;
            return this;
        }

        public int compareTo( final Duration other ) {
            if ( this.sequence > other.sequence ) {
                return 1;
            } else if ( this.sequence == other.sequence ) {
                return 0;
            }
            return -1;
        }

        static JsonSerializer<Duration> getSerializer() {
            JsonSerializer<Duration> serializer = ( src, typeOfSrc, context ) -> {
                JsonObject jsonObj = new JsonObject();
                jsonObj.addProperty( "type", src.type );
                jsonObj.addProperty( "name", src.name );
                jsonObj.add( "duration", context.serialize( src.duration ) );
                jsonObj.add( "limit", context.serialize( src.limit ) );
                jsonObj.add( "sequence", context.serialize( src.sequence ) );
                jsonObj.add( "noProgressBar", context.serialize( src.noProgressBar ) );
                Object[] children = src.children.values().toArray();
                Arrays.sort( children );
                jsonObj.add( "children", context.serialize( children ) );
                jsonObj.add( "isChild", context.serialize( src.isChild ) );
                return jsonObj;
            };
            return serializer;
        }
    }

}

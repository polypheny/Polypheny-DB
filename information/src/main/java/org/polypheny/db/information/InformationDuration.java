/*
 * Copyright 2019-2024 The Polypheny Project
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.information.exception.InformationRuntimeException;


public class InformationDuration extends Information {


    private final Map<String, Duration> children = new HashMap<>();


    @JsonProperty
    private final boolean isChild = false;

    @JsonIgnore
    private final InformationGroup group;
    /**
     * Duration in NanoSeconds
     */
    @JsonProperty
    private long duration = 0L;


    /**
     * Constructor
     *
     * @param group Group to which this InformationDuration belongs to
     */
    public InformationDuration( final InformationGroup group ) {
        super( UUID.randomUUID().toString(), group.getId() );
        this.group = group;
    }


    @JsonProperty("children")
    public Collection<Duration> getDurationValues() {
        return children.values();
    }


    public Duration start( final String name ) {
        Duration d = new Duration( name );
        this.children.put( name, d );
        return d;
    }


    public void stop( final String name ) {
        this.duration += this.children.get( name ).stop();
    }


    public InformationDuration merge( InformationDuration other ) {
        Set<String> keySet = this.children.keySet();
        keySet.retainAll( other.children.keySet() );
        if ( this.children.keySet().size() + other.children.keySet().size() != 0 && !keySet.isEmpty() ) {
            throw new RuntimeException( "It was not possible to merge the InformationDuration." );
        }
        InformationDuration duration = new InformationDuration( this.group );
        duration.children.putAll( this.children );
        duration.children.putAll( other.children );

        return duration;
    }


    public Duration get( final String name ) {
        return this.children.get( name );
    }


    public long getNanoDuration( final String name ) throws InformationRuntimeException {
        Duration child = this.get( name );
        if ( child == null ) {
            throw new InformationRuntimeException( "could no find duration: " + name );
        }
        return child.duration;
    }


    public long getDurationOrZero( final String name ) throws InformationRuntimeException {
        Duration child = this.get( name );
        if ( child == null ) {
            return 0;
        }
        return child.duration;
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


    /**
     * Helper class for Durations
     */
    public static class Duration implements Comparable<Duration> {

        static long counter = 0;
        @JsonProperty
        private final String type = InformationDuration.class.getSimpleName();//for the UI

        @JsonProperty
        private final String name;

        @JsonProperty
        private final long sequence;


        private final Map<String, Duration> children = new HashMap<>();


        @JsonProperty
        private final boolean isChild = true;
        /**
         * Duration in NanoSeconds
         */

        @JsonProperty
        private long duration;
        /**
         * If the duration is longer than the limit, the UI will indicate.
         */
        @JsonProperty
        private long limit;
        private StopWatch sw;

        @JsonProperty
        private boolean noProgressBar = false;


        private Duration( final String name ) {
            this.sequence = counter++;
            this.name = name;
            this.sw = StopWatch.createStarted();
        }


        @JsonPropertyOrder("sequence")
        @JsonProperty("children")
        public Collection<Duration> getDurationValues() {
            return children.values();
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


        @Override
        public int compareTo( final Duration other ) {
            if ( this.sequence > other.sequence ) {
                return 1;
            } else if ( this.sequence == other.sequence ) {
                return 0;
            }
            return -1;
        }

    }

}

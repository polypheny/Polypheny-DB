/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.druid;


import org.apache.calcite.avatica.util.TimeUnitRange;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.Objects;
import javax.annotation.Nonnull;

import static ch.unibas.dmi.dbis.polyphenydb.adapter.druid.DruidQuery.writeFieldIf;


/**
 * Factory methods and helpers for {@link Granularity}.
 */
public class Granularities {

    // Private constructor for utility class
    private Granularities() {
    }


    /**
     * Returns a Granularity that causes all rows to be rolled up into one.
     */
    public static Granularity all() {
        return AllGranularity.INSTANCE;
    }


    /**
     * Creates a Granularity based on a time unit.
     *
     * When used in a query, Druid will rollup and round time values based on specified period and timezone.
     */
    @Nonnull
    public static Granularity createGranularity( TimeUnitRange timeUnit, String timeZone ) {
        switch ( timeUnit ) {
            case YEAR:
                return new PeriodGranularity( Granularity.Type.YEAR, "P1Y", timeZone );
            case QUARTER:
                return new PeriodGranularity( Granularity.Type.QUARTER, "P3M", timeZone );
            case MONTH:
                return new PeriodGranularity( Granularity.Type.MONTH, "P1M", timeZone );
            case WEEK:
                return new PeriodGranularity( Granularity.Type.WEEK, "P1W", timeZone );
            case DAY:
                return new PeriodGranularity( Granularity.Type.DAY, "P1D", timeZone );
            case HOUR:
                return new PeriodGranularity( Granularity.Type.HOUR, "PT1H", timeZone );
            case MINUTE:
                return new PeriodGranularity( Granularity.Type.MINUTE, "PT1M", timeZone );
            case SECOND:
                return new PeriodGranularity( Granularity.Type.SECOND, "PT1S", timeZone );
            default:
                throw new AssertionError( timeUnit );
        }
    }


    /**
     * Implementation of {@link Granularity} for {@link Granularity.Type#ALL}. A singleton.
     */
    private enum AllGranularity implements Granularity {
        INSTANCE;


        @Override
        public void write( JsonGenerator generator ) throws IOException {
            generator.writeObject( "all" );
        }


        @Nonnull
        public Type getType() {
            return Type.ALL;
        }
    }


    /**
     * Implementation of {@link Granularity} based on a time unit. Corresponds to PeriodGranularity in Druid.
     */
    private static class PeriodGranularity implements Granularity {

        private final Type type;
        private final String period;
        private final String timeZone;


        private PeriodGranularity( Type type, String period, String timeZone ) {
            this.type = Objects.requireNonNull( type );
            this.period = Objects.requireNonNull( period );
            this.timeZone = Objects.requireNonNull( timeZone );
        }


        @Override
        public void write( JsonGenerator generator ) throws IOException {
            generator.writeStartObject();
            generator.writeStringField( "type", "period" );
            DruidQuery.writeFieldIf( generator, "period", period );
            DruidQuery.writeFieldIf( generator, "timeZone", timeZone );
            generator.writeEndObject();
        }


        @Nonnull
        public Type getType() {
            return type;
        }
    }
}


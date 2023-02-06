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

package org.polypheny.db.adapter.cottontail.util;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import java.math.BigDecimal;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;


public class CottontailSerialisation {

    public static final Gson GSON;

    public static final JsonSerializer<DateString> DATE_STRING_JSON_SERIALIZER =
            ( src, typeOfSrc, context ) -> new JsonPrimitive( src.getDaysSinceEpoch() );

    public static final JsonDeserializer<DateString> DATE_STRING_JSON_DESERIALIZER =
            ( json, typeOfT, context ) -> DateString.fromDaysSinceEpoch( json.getAsInt() );

    public static final JsonSerializer<TimeString> TIME_STRING_JSON_SERIALIZER =
            ( src, typeOfSrc, context ) -> new JsonPrimitive( src.getMillisOfDay() );

    public static final JsonDeserializer<TimeString> TIME_STRING_JSON_DESERIALIZER =
            ( json, typeOfT, context ) -> TimeString.fromMillisOfDay( json.getAsInt() );

    public static final JsonSerializer<TimestampString> TIMESTAMP_STRING_JSON_SERIALIZER =
            ( src, typeOfSrc, context ) -> new JsonPrimitive( src.getMillisSinceEpoch() );

    public static final JsonDeserializer<TimestampString> TIMESTAMP_STRING_JSON_DESERIALIZER =
            ( json, typeOfT, context ) -> TimestampString.fromMillisSinceEpoch( json.getAsLong() );

    public static final JsonSerializer<BigDecimal> BIG_DECIMAL_JSON_SERIALIZER =
            ( src, typeOfSrc, context ) -> new JsonPrimitive( src.toString() );

    public static final JsonDeserializer<BigDecimal> BIG_DECIMAL_JSON_DESERIALIZER =
            ( json, typeOfT, context ) -> new BigDecimal( json.getAsString() );


    static {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter( DateString.class, DATE_STRING_JSON_SERIALIZER );
        gsonBuilder.registerTypeAdapter( DateString.class, DATE_STRING_JSON_DESERIALIZER );
        gsonBuilder.registerTypeAdapter( TimeString.class, TIME_STRING_JSON_SERIALIZER );
        gsonBuilder.registerTypeAdapter( TimeString.class, TIME_STRING_JSON_DESERIALIZER );
        gsonBuilder.registerTypeAdapter( TimestampString.class, TIMESTAMP_STRING_JSON_SERIALIZER );
        gsonBuilder.registerTypeAdapter( TimestampString.class, TIMESTAMP_STRING_JSON_DESERIALIZER );
        gsonBuilder.registerTypeAdapter( BigDecimal.class, BIG_DECIMAL_JSON_SERIALIZER );
        gsonBuilder.registerTypeAdapter( BigDecimal.class, BIG_DECIMAL_JSON_DESERIALIZER );

        GSON = gsonBuilder.create();
    }

}

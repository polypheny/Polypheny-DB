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

package org.polypheny.db.cli;

import com.github.rvesse.airline.model.ArgumentsMetadata;
import com.github.rvesse.airline.model.OptionMetadata;
import com.github.rvesse.airline.parser.ParseState;
import com.github.rvesse.airline.types.DefaultTypeConverter;
import com.github.rvesse.airline.types.TypeConverter;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.util.RunMode;

@Slf4j
public class PolyModesConverter extends DefaultTypeConverter {

    @Override
    public <T> TypeConverter getTypeConverter( OptionMetadata option, ParseState<T> state ) {
        return this;
    }


    @Override
    public <T> TypeConverter getTypeConverter( ArgumentsMetadata arguments, ParseState<T> state ) {
        return this;
    }


    @Override
    public Object convert( String name, Class<?> type, String value ) {
        String adjustedName = value.toUpperCase();

        if ( Arrays.stream( RunMode.values() ).anyMatch( v -> v.name().equals( adjustedName ) ) ) {
            return RunMode.valueOf( adjustedName.toUpperCase() );
        }

        switch ( adjustedName.toLowerCase() ) {
            case "t":
                return RunMode.TEST;
            case "b":
            case "bench":
                return RunMode.BENCHMARK;
            case "d":
            case "dev":
                return RunMode.DEVELOPMENT;
        }
        log.warn( "Could not find the mode: " + adjustedName );
        return RunMode.PRODUCTION;
    }

}

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

package org.polypheny.db.cli;

import com.github.rvesse.airline.types.DefaultTypeConverter;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog.PolyphenyMode;

@Slf4j
public class PolyphenyModesConverter extends DefaultTypeConverter {

    @Override
    public Object convert( String name, Class<?> type, String value ) {
        String adjustedName = name.toUpperCase();

        if ( Arrays.stream( PolyphenyMode.values() ).anyMatch( v -> v.name().equals( adjustedName ) ) ) {
            return PolyphenyMode.valueOf( adjustedName );
        }

        switch ( adjustedName ) {
            case "T":
                return PolyphenyMode.TEST;
            case "B":
                return PolyphenyMode.BENCHMARK;
            case "D":
                return PolyphenyMode.DEFAULT;
        }
        log.warn( "Could not find the mode: " + adjustedName );
        return PolyphenyMode.DEFAULT;
    }

}

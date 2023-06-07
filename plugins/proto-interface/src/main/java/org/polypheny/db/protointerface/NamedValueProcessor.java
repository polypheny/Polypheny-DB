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

package org.polypheny.db.protointerface;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import org.polypheny.db.type.entity.PolyValue;

public class NamedValueProcessor {

    // matches tags such as :name
    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile( "(?<!')(:[\\w]*)(?!')" );
    private static final String REPLACEMENT_CHARACTER = "?";
    @Getter
    private String statement;
    @Getter
    private List<PolyValue> values;


    public void
    process( String statement, Map<String, PolyValue> valueMap ) {
        values = new LinkedList<>();
        Matcher matcher = PLACEHOLDER_PATTERN.matcher( statement );
        if ( !matcher.find() ) {
            this.statement = statement;
        }
        StringBuilder stringBuilder = new StringBuilder();
        String currentGroup = matcher.group( 1 );
        do {
            if ( !valueMap.containsKey( currentGroup ) ) {
                throw new ProtoInterfaceServiceException( "Missing value for named parameter:" + currentGroup );
            }
            matcher.appendReplacement( stringBuilder, REPLACEMENT_CHARACTER );
            values.add( valueMap.get( currentGroup ) );
        } while ( matcher.find() );
        matcher.appendTail( stringBuilder );
        this.statement = stringBuilder.toString();
    }

}

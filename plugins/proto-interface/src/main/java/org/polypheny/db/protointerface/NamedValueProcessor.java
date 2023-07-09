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

import com.google.common.collect.ImmutableBiMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import org.polypheny.db.type.entity.PolyValue;

public class NamedValueProcessor {

    // matches tags such as :name
    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile( "(?<!')(:[\\w]*)(?!')" );
    private static final String REPLACEMENT_CHARACTER = "?";
    @Getter
    private String processedQuery;
    @Getter
    private ImmutableBiMap<String, Integer> namedIndexes;


    public NamedValueProcessor( String statement ) {
        this.processStatement( statement );
    }


    public void processStatement( String statement ) {
        HashMap<String, Integer> indexByName = new HashMap<>();
        AtomicInteger valueIndex = new AtomicInteger();
        Matcher matcher = PLACEHOLDER_PATTERN.matcher( statement );
        if ( !matcher.find() ) {
            this.processedQuery = statement;
        }
        StringBuilder stringBuilder = new StringBuilder();
        String currentGroup = matcher.group( 1 );
        do {
            matcher.appendReplacement( stringBuilder, REPLACEMENT_CHARACTER );
            indexByName.put( currentGroup, valueIndex.getAndIncrement() );
        } while ( matcher.find() );
        matcher.appendTail( stringBuilder );
        this.processedQuery = stringBuilder.toString();
        this.namedIndexes = ImmutableBiMap.copyOf( indexByName );
    }


    public List<PolyValue> transformValueMap( Map<String, PolyValue> values ) {
        List<PolyValue> image = new ArrayList<>();
        values.forEach( ( key, value ) -> image.set( namedIndexes.get( key ), value ) );
        return image;
    }

}

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

import io.grpc.LoadBalancer.CreateSubchannelArgs;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.polypheny.db.type.entity.PolyValue;

public class PlaceholderReplacement {

    // matches tags such as :name
    private static final Pattern placeholderpattern = Pattern.compile( "(?<!')(:[\\w]*)(?!')" );


    public static String replacePlacehoders( String statement, Map<String, PolyValue> values ) {
        Matcher matcher = placeholderpattern.matcher( statement );
        if ( !matcher.find() ) {
            return statement;
        }
        StringBuilder stringBuilder = new StringBuilder();
        String currentGroup = matcher.group( 1 );
        do {
            if ( !values.containsKey( currentGroup ) ) {
                throw new ProtoInterfaceServiceException( "Missing value for named parameter :" + currentGroup );
            }
            matcher.appendReplacement( stringBuilder, values.get( currentGroup ).toString() );
        } while ( matcher.find() );
        matcher.appendTail( stringBuilder );
        return stringBuilder.toString();
    }

}

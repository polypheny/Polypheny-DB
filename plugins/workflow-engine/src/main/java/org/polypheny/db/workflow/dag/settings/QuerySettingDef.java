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

package org.polypheny.db.workflow.dag.settings;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Arrays;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.annotations.QuerySetting;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointQuery;

@EqualsAndHashCode(callSuper = true)
@Value
public class QuerySettingDef extends SettingDef {

    String[] queryLanguages;
    String entityL = CheckpointQuery.ENTITY_L;
    String entityR = CheckpointQuery.ENTITY_R;


    public QuerySettingDef( QuerySetting a ) {
        super( SettingType.QUERY, a.key(), a.displayName(), a.shortDescription(), a.longDescription(), getDefaultValue( a.queryLanguages() ),
                a.group(), a.subGroup(), a.pos(), a.subPointer(), a.subValues() );
        this.queryLanguages = a.queryLanguages();
    }


    @Override
    public SettingValue buildValue( JsonNode node ) {
        return QueryValue.of( node );
    }


    @Override
    public void validateValue( SettingValue value ) throws InvalidSettingException {
        if ( value instanceof QueryValue queryValue ) {
            String language = queryValue.getQueryLanguage();
            if ( Arrays.stream( queryLanguages ).noneMatch( language::equals ) ) {
                throwInvalid( "Invalid query language: " + language );
            } else if ( queryValue.getQuery().isBlank() ) {
                throwInvalid( "Query is empty" );
            }
            return;
        }
        throw new IllegalArgumentException( "Value is not a QueryValue" );

    }


    private static SettingValue getDefaultValue( String[] queryLanguages ) {
        return new QueryValue( "", queryLanguages[0] );
    }

}

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
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.annotations.GraphMapSetting;

@EqualsAndHashCode(callSuper = true)
@Value
public class GraphMapSettingDef extends SettingDef {

    boolean canExtendGraph;
    int targetInput; // since it's a multi-input, the index of the first input
    int graphInput; // the index of the graph, in case canExtendGraph is true


    public GraphMapSettingDef( GraphMapSetting a ) {
        super( SettingType.GRAPH_MAP, a.key(), a.displayName(), a.shortDescription(), a.longDescription(), constructDefaultValue(),
                a.group(), a.subGroup(), a.pos(), a.subPointer(), a.subValues() );
        this.canExtendGraph = a.canExtendGraph();
        this.targetInput = a.targetInput();
        this.graphInput = a.graphInput();
    }


    @Override
    public SettingValue buildValue( JsonNode node ) {
        return SettingValue.fromJson( node, GraphMapValue.class );
    }


    @Override
    public void validateValue( SettingValue value ) throws InvalidSettingException {
        if ( value instanceof GraphMapValue map ) {
            try {
                map.validate( Integer.MAX_VALUE, canExtendGraph );
            } catch ( IllegalArgumentException e ) {
                throwInvalid( e.getMessage() );
            }
            return;
        }
        throw new IllegalArgumentException( "Value is not a GraphMapValue" );

    }


    private static SettingValue constructDefaultValue() {
        return new GraphMapValue( List.of() );
    }

}

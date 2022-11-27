/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.schema.graph;

import com.google.gson.annotations.Expose;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.runtime.PolyCollections.PolyDictionary;
import org.polypheny.db.runtime.PolyCollections.PolyList;
import org.polypheny.db.type.BasicPolyType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.NlsString;


@Getter
public abstract class GraphPropertyHolder extends GraphObject {

    @Expose
    public final PolyDictionary properties;
    @Expose
    public final PolyList<String> labels;


    public GraphPropertyHolder( String id, GraphObjectType type, PolyDictionary properties, List<String> labels, String variableName ) {
        super( id, type, variableName );
        this.properties = properties;
        this.labels = new PolyList<>( labels );
    }


    public PolyList<RexLiteral> getRexLabels() {
        return labels
                .stream()
                .map( l -> new RexLiteral( new NlsString( l, StandardCharsets.ISO_8859_1.name(), Collation.IMPLICIT ), new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.VARCHAR, 255 ), PolyType.CHAR ) )
                .collect( Collectors.toCollection( PolyList::new ) );
    }


    public boolean matchesProperties( PolyDictionary properties ) {
        for ( Entry<String, Object> entry : properties.entrySet() ) {
            if ( !this.properties.containsKey( entry.getKey() ) ) {
                return false;
            }
            if ( !this.properties.get( entry.getKey() ).toString().equals( entry.getValue().toString() ) ) {
                return false;
            }
        }
        return true;
    }


    public boolean matchesLabels( List<String> labels ) {
        return this.labels.equals( labels );
    }


    public boolean labelAndPropertyMatch( GraphPropertyHolder other ) {
        // we don't need to match the source and target, this is done via segments or paths
        if ( !other.properties.isEmpty() ) {
            if ( !matchesProperties( other.properties ) ) {
                return false;
            }
        }
        if ( !other.labels.isEmpty() ) {
            if ( !matchesLabels( other.labels ) ) {
                return false;
            }
        }

        return true;
    }


    public abstract void setLabels( List<String> labels );

}

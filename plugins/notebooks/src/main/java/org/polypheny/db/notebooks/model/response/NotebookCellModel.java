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

package org.polypheny.db.notebooks.model.response;

import com.google.gson.annotations.SerializedName;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class NotebookCellModel {

    @Getter
    @SerializedName("cell_type")
    private String cellType;

    @Getter
    private String id;

    @Getter
    private Object source; // String[] or String

    @Getter
    private CellMetadataModel metadata;

    @Getter
    @SerializedName("execution_count")
    private int executionCount;

    @Getter
    private List<Object> outputs;


    public NotebookCellModel( String code ) {
        this( code, new LinkedList<>() );
    }


    public NotebookCellModel( String code, List<Object> outputs ) {
        this.cellType = "code";
        this.id = UUID.randomUUID().toString();
        this.source = code;
        this.metadata = new CellMetadataModel();
        this.outputs = outputs;
    }


    public String getSourceAsString() {
        if ( source instanceof String[] ) {
            String[] sourceArray = (String[]) source;
            return String.join( "", sourceArray );
        } else if ( source instanceof String ) {
            return (String) source;
        } else {
            return null;
        }
    }


    public boolean isPolyCell() {
        if ( metadata != null && metadata.getPolypheny() != null && metadata.getPolypheny().getCellType() != null ) {
            return metadata.getPolypheny().getCellType().equals( "poly" );
        }
        return false;
    }


    public PolyMetadataModel getPolyMetadata() {
        if ( metadata != null ) {
            return metadata.getPolypheny();
        }
        return null;
    }

}

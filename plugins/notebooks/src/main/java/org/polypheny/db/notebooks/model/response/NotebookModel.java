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
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.polypheny.db.notebooks.model.language.JupyterKernelLanguage;

/**
 * See https://nbformat.readthedocs.io/en/latest/format_description.html
 */
@NoArgsConstructor
public class NotebookModel {

    @Getter
    @Setter
    private List<NotebookCellModel> cells;

    @Getter
    private Object metadata;

    @Getter
    private int nbformat;

    @Getter
    @SerializedName("nbformat_minor")
    private int nbformatMinor;


    /**
     * Using the provided JupyterKernelLanguage, poly-cells are converted to normal code cells that
     * should work in other Jupyter Frontends as Polypheny (provided the necessary packages are installed).
     *
     * @param exporter the JupyterKernelLanguage instance to be used for the conversion
     */
    public void exportCells( JupyterKernelLanguage exporter ) {
        List<NotebookCellModel> exportedCells = new LinkedList<>();
        boolean hasPolyCell = false;

        for ( NotebookCellModel cell : this.cells ) {
            if ( cell.isPolyCell() ) {
                PolyMetadataModel polyMeta = cell.getPolyMetadata();
                String source = cell.getSourceAsString();
                if ( source == null ) {
                    continue;
                }
                hasPolyCell = true;

                List<String> codeParts = exporter.exportedQuery( source, polyMeta.getLanguage(), polyMeta.getNamespace(),
                        polyMeta.getResultVariable(), polyMeta.getExpandParams() );
                for ( int i = 0; i < codeParts.size() - 1; i++ ) {
                    exportedCells.add( new NotebookCellModel( codeParts.get( i ) ) );
                }
                // add possible resultSet in json format to last generated cell
                exportedCells.add( new NotebookCellModel( codeParts.get( codeParts.size() - 1 ), cell.getOutputs() ) );
            } else {
                exportedCells.add( cell );
            }
        }

        if ( hasPolyCell ) {
            List<String> initCodeParts = exporter.getExportedInitCode();
            for ( int i = initCodeParts.size() - 1; i >= 0; i-- ) {
                exportedCells.add( 0, new NotebookCellModel( initCodeParts.get( i ) ) );
            }
            // replace old cells with exported cells
            cells = exportedCells;
        }
    }

}

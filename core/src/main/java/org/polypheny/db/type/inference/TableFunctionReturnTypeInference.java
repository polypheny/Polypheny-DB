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

package org.polypheny.db.type.inference;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.polypheny.db.algebra.metadata.AlgColumnMapping;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.nodes.CallBinding;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Static;


/**
 * TableFunctionReturnTypeInference implements rules for deriving table function output row types by expanding references to
 * cursor parameters.
 */
public class TableFunctionReturnTypeInference extends ExplicitReturnTypeInference {

    private final List<String> paramNames;

    private Set<AlgColumnMapping> columnMappings; // not re-entrant!

    private final boolean isPassthrough;


    public TableFunctionReturnTypeInference( AlgProtoDataType unexpandedOutputType, List<String> paramNames, boolean isPassthrough ) {
        super( unexpandedOutputType );
        this.paramNames = paramNames;
        this.isPassthrough = isPassthrough;
    }


    public Set<AlgColumnMapping> getColumnMappings() {
        return columnMappings;
    }


    @Override
    public AlgDataType inferReturnType( OperatorBinding opBinding ) {
        columnMappings = new HashSet<>();
        AlgDataType unexpandedOutputType = protoType.apply( opBinding.getTypeFactory() );
        List<AlgDataType> expandedOutputTypes = new ArrayList<>();
        List<String> expandedFieldNames = new ArrayList<>();
        for ( AlgDataTypeField field : unexpandedOutputType.getFields() ) {
            AlgDataType fieldType = field.getType();
            String fieldName = field.getName();
            if ( fieldType.getPolyType() != PolyType.CURSOR ) {
                expandedOutputTypes.add( fieldType );
                expandedFieldNames.add( fieldName );
                continue;
            }

            // Look up position of cursor parameter with same name as output field, also counting how many cursors appear
            // before it (need this for correspondence with {@link AlgNode} child position).
            int paramOrdinal = -1;
            int iCursor = 0;
            for ( int i = 0; i < paramNames.size(); ++i ) {
                if ( paramNames.get( i ).equals( fieldName ) ) {
                    paramOrdinal = i;
                    break;
                }
                AlgDataType cursorType = opBinding.getCursorOperand( i );
                if ( cursorType != null ) {
                    ++iCursor;
                }
            }
            assert paramOrdinal != -1;

            // Translate to actual argument type.
            boolean isRowOp = false;
            List<String> columnNames = new ArrayList<>();
            AlgDataType cursorType = opBinding.getCursorOperand( paramOrdinal );
            if ( cursorType == null ) {
                isRowOp = true;
                String parentCursorName = opBinding.getColumnListParamInfo( paramOrdinal, fieldName, columnNames );
                assert parentCursorName != null;
                paramOrdinal = -1;
                iCursor = 0;
                for ( int i = 0; i < paramNames.size(); ++i ) {
                    if ( paramNames.get( i ).equals( parentCursorName ) ) {
                        paramOrdinal = i;
                        break;
                    }
                    cursorType = opBinding.getCursorOperand( i );
                    if ( cursorType != null ) {
                        ++iCursor;
                    }
                }
                cursorType = opBinding.getCursorOperand( paramOrdinal );
                assert cursorType != null;
            }

            // And expand. Function output is always nullable... except system fields.
            int iInputColumn;
            if ( isRowOp ) {
                for ( String columnName : columnNames ) {
                    iInputColumn = -1;
                    AlgDataTypeField cursorField = null;
                    for ( AlgDataTypeField cField : cursorType.getFields() ) {
                        ++iInputColumn;
                        if ( cField.getName().equals( columnName ) ) {
                            cursorField = cField;
                            break;
                        }
                    }
                    addOutputColumn( expandedFieldNames, expandedOutputTypes, iInputColumn, iCursor, opBinding, cursorField );
                }
            } else {
                iInputColumn = -1;
                for ( AlgDataTypeField cursorField : cursorType.getFields() ) {
                    ++iInputColumn;
                    addOutputColumn( expandedFieldNames, expandedOutputTypes, iInputColumn, iCursor, opBinding, cursorField );
                }
            }
        }
        return opBinding.getTypeFactory().createStructType( null, expandedOutputTypes, expandedFieldNames );
    }


    private <T extends OperatorBinding & CallBinding> void addOutputColumn(
            List<String> expandedFieldNames,
            List<AlgDataType> expandedOutputTypes,
            int iInputColumn,
            int iCursor,
            OperatorBinding opBinding,
            AlgDataTypeField cursorField ) {
        columnMappings.add( new AlgColumnMapping( expandedFieldNames.size(), iCursor, iInputColumn, !isPassthrough ) );

        // As a special case, system fields are implicitly NOT NULL. A badly behaved UDX can still provide NULL values,
        // so the system must ensure that each generated system field has a reasonable value.
        boolean nullable = true;
        if ( opBinding instanceof CallBinding ) {
            CallBinding sqlCallBinding = (CallBinding) opBinding;
            if ( sqlCallBinding.getValidator().isSystemField( cursorField ) ) {
                nullable = false;
            }
        }
        AlgDataType nullableType = opBinding.getTypeFactory().createTypeWithNullability( cursorField.getType(), nullable );

        // Make sure there are no duplicates in the output column names
        for ( String fieldName : expandedFieldNames ) {
            if ( fieldName.equals( cursorField.getName() ) ) {
                throw opBinding.newError( Static.RESOURCE.duplicateColumnName( cursorField.getName() ) );
            }
        }
        expandedOutputTypes.add( nullableType );
        expandedFieldNames.add( cursorField.getName() );
    }

}


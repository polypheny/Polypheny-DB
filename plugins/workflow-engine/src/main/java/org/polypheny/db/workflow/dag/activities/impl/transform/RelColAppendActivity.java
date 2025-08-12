/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.workflow.dag.activities.impl.transform;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.RelType;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@Slf4j
@ActivityDefinition(type = "relColAppend", displayName = "Append Columns", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.REL), @InPort(type = PortType.REL) },
        outPorts = { @OutPort(type = PortType.REL) },
        shortDescription = "Appends the columns of the second input to the columns of the first input. A strategy can be specified on how to handle mismatched row counts."
)

@EnumSetting(key = "mode", displayName = "Mismatched Row Counts", options = { "null", "default", "skip", "fail" }, defaultValue = "fail",
        displayOptions = { "Insert Null", "Insert Default", "Ignore", "Fail Execution" },
        shortDescription = "Determines the strategy for handling non-matching input row counts. 'Ignore' only outputs as many rows as there are in the smaller table. 'Insert Null' requires nullable types for all columns in the smaller table.")

@SuppressWarnings("unused")
public class RelColAppendActivity implements Activity, Pipeable {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( inTypes.stream().anyMatch( TypePreview::isEmpty ) ) {
            return UnknownType.ofRel().asOutTypes();
        }
        return RelType.of( getType( inTypes.get( 0 ).getNullableType(), inTypes.get( 1 ).getNullableType() ) ).asOutTypes();
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getType( inTypes.get( 0 ), inTypes.get( 1 ) );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        Iterator<List<PolyValue>> it0 = inputs.get( 0 ).iterator();
        Iterator<List<PolyValue>> it1 = inputs.get( 1 ).iterator();
        AlgDataType type0 = inputs.get( 0 ).getType();
        AlgDataType type1 = ActivityUtils.removeField( inputs.get( 1 ).getType(), StorageManager.PK_COL );
        int fieldCount = output.getType().getFieldCount();

        boolean hasCheckedNull = false;
        String mode = settings.getString( "mode" );
        while ( true ) {
            boolean next0 = it0.hasNext();
            boolean next1 = it1.hasNext();
            List<PolyValue> out = new ArrayList<>( fieldCount );
            if ( next0 && next1 ) {
                out.addAll( it0.next() );
                List<PolyValue> row = it1.next();
                out.addAll( row.subList( 1, row.size() ) );
            } else if ( mode.equals( "skip" ) || (!next0 && !next1) ) {
                if ( next0 || next1 ) {
                    ctx.logInfo( "Skipping remaining rows" );
                }
                finish( inputs );
                return;
            } else if ( mode.equals( "fail" ) ) {
                String largerInput = next0 ? "input0" : "input1";
                throw new IllegalArgumentException( "Mismatched Row Counts: '" + largerInput + "' has too many rows" );
            } else if ( mode.equals( "default" ) ) {
                if ( next0 ) {
                    out.addAll( it0.next() );
                    out.addAll( getDefaultRow( type1 ) ); // type1 already has PK_COL removed
                } else {
                    out.addAll( getDefaultRow( type0 ) );
                    List<PolyValue> row = it1.next();
                    out.addAll( row.subList( 1, row.size() ) );
                }
            } else {
                assert mode.equals( "null" );
                if ( !hasCheckedNull ) {
                    if ( !allFieldsNull( next1 ? type0 : type1 ) ) {
                        String largerInput = next1 ? "input0" : "input1";
                        throw new IllegalArgumentException( "Unable to insert NULL into NOT NULL column for '" + largerInput + "'" );
                    }
                    hasCheckedNull = true;
                }
                if ( next0 ) {
                    out.addAll( it0.next() );
                    out.addAll( getNullRow( type1 ) ); // type1 already has PK_COL removed
                } else {
                    out.addAll( getNullRow( type0 ) );
                    List<PolyValue> row = it1.next();
                    out.addAll( row.subList( 1, row.size() ) );
                }
            }
            if ( !output.put( out ) ) {
                finish( inputs );
                return;
            }
        }
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Pipeable.super.execute( inputs, settings, ctx );
    }


    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        long first = inCounts.get( 0 );
        long second = inCounts.get( 1 );

        return switch ( settings.getString( "mode" ) ) {
            case "null", "default" -> Math.max( first, second );
            default -> Math.min( first, second );
        };
    }


    private AlgDataType getType( AlgDataType type0, AlgDataType type1 ) {
        return ActivityUtils.concatTypes(
                type0,
                ActivityUtils.removeField( type1, StorageManager.PK_COL )
        );
    }


    private List<PolyValue> getNullRow( AlgDataType type ) {
        List<PolyValue> row = new ArrayList<>();
        for ( AlgDataTypeField field : type.getFields() ) {
            Class<? extends PolyValue> clazz = PolyValue.classFrom( field.getType().getPolyType() );
            PolyValue nullValue = PolyNull.getNull( clazz );
            row.add( nullValue );
        }
        return row;
    }


    private List<PolyValue> getDefaultRow( AlgDataType type ) {
        List<PolyValue> row = new ArrayList<>();
        for ( AlgDataTypeField field : type.getFields() ) {
            PolyType polyType = field.getType().getPolyType();
            if ( polyType.getFamily() == PolyTypeFamily.CHARACTER ) {
                row.add( PolyString.of( "" ) );
            } else {
                PolyValue value = PolyValue.getInitial( PolyValue.classFrom( polyType ) );
                if ( value == null ) {
                    throw new IllegalArgumentException( "Unable to get default value for field '" + field.getName() + "' with type '" + polyType + "'" );
                }
                row.add( value );
            }
        }
        return row;
    }


    private boolean allFieldsNull( AlgDataType type ) {
        for ( AlgDataTypeField field : type.getFields() ) {
            if ( !field.getType().isNullable() && !field.getName().equals( StorageManager.PK_COL ) ) {
                return false;
            }
        }
        return true;
    }

}

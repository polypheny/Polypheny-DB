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

package org.polypheny.db.workflow.dag.activities.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidInputException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.LpgType;
import org.polypheny.db.workflow.dag.activities.TypePreview.RelType;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.DefaultGroup;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.annotations.Group.Subgroup;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.EnumSettingDef.EnumStyle;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.GroupDef;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringSettingDef.AutoCompleteType;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointQuery;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointQuery.CheckpointQueryBuilder;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.DocReader;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;

@ActivityDefinition(type = "relLookup", displayName = "Table Value Lookup", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL, ActivityCategory.DOCUMENT },
        inPorts = {
                @InPort(type = PortType.REL, description = "The input table."),
                @InPort(type = PortType.ANY, description = "The lookup table or collection.") },
        outPorts = { @OutPort(type = PortType.REL) },
        shortDescription = "Define column(s) in the input table that are used as key(s) for looking up value(s) in a lookup table or collection. In case of multiple matches, the first one is used."
)
@DefaultGroup(subgroups = {
        @Subgroup(key = "key", displayName = "Key"),
        @Subgroup(key = "value", displayName = "Value")
})

@FieldSelectSetting(key = "leftFields", displayName = "Input Key(s)", simplified = true, reorder = true, targetInput = 0,
        subGroup = "key", pos = 0,
        shortDescription = "Specify the columns(s) in the input table that contain the key to look up.")
@FieldSelectSetting(key = "rightFields", displayName = "Lookup Key(s)", simplified = true, reorder = true, targetInput = 1,
        subGroup = "key", pos = 1,
        shortDescription = "Specify the key field(s) in the lookup input.")

@FieldSelectSetting(key = "valueFields", displayName = "Value Field(s)", simplified = true, reorder = true, targetInput = 1,
        subGroup = "value", pos = 0,
        shortDescription = "Specify the field(s) in the lookup input that contain the value(s) to insert.")
@StringSetting(key = "target", displayName = "Add Prefix", defaultValue = "lookup_",
        subGroup = "value", pos = 1,
        autoCompleteType = AutoCompleteType.FIELD_NAMES, autoCompleteInput = 0, maxLength = 1024,
        shortDescription = "Add an optional prefix to the value columns to be appended to the input table.")

@BoolSetting(key = "keepKeys", displayName = "Keep Key Column(s)", defaultValue = true,
        group = GroupDef.ADVANCED_GROUP, pos = 0)
@EnumSetting(key = "matchType", displayName = "Match Type", style = EnumStyle.RADIO_BUTTON, pos = 1,
        options = { "EXACT", "SMALLER", "LARGER" },
        displayOptions = { "Equal", "Next Smaller", "Next Larger" },
        displayDescriptions = { "Keys must match exactly.", "If no matching key is found, use the value of the next smaller key.", "If no matching key is found, use the value of the next larger key." },
        defaultValue = "EXACT", group = GroupDef.ADVANCED_GROUP)
@BoolSetting(key = "fail", displayName = "Fail on Missing Key", defaultValue = false,
        group = GroupDef.ADVANCED_GROUP, pos = 2,
        shortDescription = "If true, attempting to look up a key that does not exist fails the execution.")

@SuppressWarnings("unused")
public class RelLookupActivity implements Activity {

    private static final int MAX_CACHE_SIZE = 100_000;
    private final Map<List<PolyValue>, List<PolyValue>> lookupCache = new HashMap<>();
    private final NavigableMap<PolyValue, List<PolyValue>> orderedLookupCache = new TreeMap<>();
    private final List<PolyValue> emptyValue = new ArrayList<>();
    private CheckpointQueryBuilder queryBuilder;

    private CheckpointReader reader;
    private String matchType;
    private List<String> leftFields;
    private List<String> rightFields;
    private List<String> valueFields;
    private boolean fail;


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        TypePreview left = inTypes.get( 0 ), right = inTypes.get( 1 );

        if ( right instanceof LpgType ) {
            throw new InvalidInputException( "Only a table or collection can be used as lookup input", 1 );
        }

        if ( inTypes.stream().anyMatch( t -> !t.isPresent() ) || !settings.keysPresent( "leftFields", "keepKeys", "rightFields", "valueFields", "matchType" ) ) {
            return UnknownType.ofRel().asOutTypes();
        }
        List<String> leftFields = settings.getOrThrow( "leftFields", FieldSelectValue.class ).getInclude();
        List<String> rightFields = settings.getOrThrow( "rightFields", FieldSelectValue.class ).getInclude();
        List<String> values = settings.getOrThrow( "valueFields", FieldSelectValue.class ).getInclude();
        String prefix = Objects.requireNonNullElse( settings.getNullableString( "target" ), "" );
        AlgDataType inType = left.getNullableType();
        AlgDataType relType = right.getDataModel() == DataModel.RELATIONAL ? right.getNullableType() : null;

        validate( leftFields, rightFields, values, inType, relType, settings.getString( "matchType" ) );
        AlgDataType outType = getType( leftFields, values, inType, relType, prefix, settings.getBool( "keepKeys" ) );
        return RelType.of( outType ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        AlgDataType inType = inputs.get( 0 ).getTupleType();
        reader = inputs.get( 1 );
        boolean isCollection = reader.getDataModel() == DataModel.DOCUMENT;
        matchType = settings.getString( "matchType" );
        leftFields = settings.get( "leftFields", FieldSelectValue.class ).getInclude();
        rightFields = settings.get( "rightFields", FieldSelectValue.class ).getInclude();
        valueFields = settings.get( "valueFields", FieldSelectValue.class ).getInclude();
        emptyValue.addAll( valueFields.stream().map( s -> PolyNull.NULL ).toList() );
        fail = settings.getBool( "fail" );
        boolean keepKeys = settings.getBool( "keepKeys" );
        String prefix = Objects.requireNonNullElse( settings.getString( "target" ), "" );

        AlgDataType type = getType( leftFields, valueFields, inType, reader.getTupleType(), prefix, keepKeys );
        List<Integer> keyIndices = leftFields.stream().map( f -> inType.getFieldNames().indexOf( f ) ).toList();

        if ( isCollection ) {
            populateCache( (DocReader) reader );
        }

        RelWriter writer = ctx.createRelWriter( 0, type );
        long inCount = inputs.get( 0 ).getTupleCount();
        long countDelta = Math.max( inCount / 100, 1 );
        long count = 0;
        for ( List<PolyValue> row : inputs.get( 0 ).getIterable() ) {
            List<PolyValue> keys = keyIndices.stream().map( row::get ).toList();
            List<PolyValue> values = isCollection ? lookupCollection( keys ) : lookupTable( keys );
            if ( values.size() != valueFields.size() ) {
                throw new GenericRuntimeException( "Expected " + valueFields.size() + " values but got " + values.size() );
            }

            List<PolyValue> outRow = new ArrayList<>();
            for ( int i = 0; i < row.size(); i++ ) {
                if ( !keepKeys && keyIndices.contains( i ) ) {
                    continue;
                }
                outRow.add( row.get( i ) );
            }
            outRow.addAll( values );

            writer.write( outRow );
            count++;
            if ( count % countDelta == 0 ) {
                ctx.updateProgress( (double) count / inCount );
            }
            ctx.checkInterrupted();
        }
    }


    @Override
    public void reset() {
        reader = null;
        matchType = null;
        rightFields = null;
        valueFields = null;
        fail = false;
        queryBuilder = null;
        lookupCache.clear();
        orderedLookupCache.clear();
        emptyValue.clear();
    }


    private List<PolyValue> lookupTable( List<PolyValue> keys ) throws Exception {
        if ( lookupCache.containsKey( keys ) ) {
            return lookupCache.get( keys );
        } else {
            List<PolyValue> value = retrieveTableValue( keys );
            if ( lookupCache.size() > MAX_CACHE_SIZE ) {
                lookupCache.remove( lookupCache.keySet().iterator().next() );
            }
            lookupCache.put( keys, value );
            return value;
        }
    }


    private List<PolyValue> lookupCollection( List<PolyValue> keys ) {
        List<PolyValue> value = switch ( matchType ) {
            case "EXACT" -> {
                List<PolyValue> v = lookupCache.get( keys );
                yield v == null ? List.of() : v;
            }
            case "SMALLER" -> {
                Entry<PolyValue, List<PolyValue>> entry = orderedLookupCache.floorEntry( keys.get( 0 ) );
                yield entry == null ? List.of() : entry.getValue();
            }
            case "LARGER" -> {
                Entry<PolyValue, List<PolyValue>> entry = orderedLookupCache.ceilingEntry( keys.get( 0 ) );
                yield entry == null ? List.of() : entry.getValue();
            }
            default -> throw new IllegalStateException( "Unexpected value: " + matchType );
        };
        if ( value.isEmpty() ) {
            if ( fail ) {
                throw new GenericRuntimeException( "Unable to lookup value for '" + keys + "'" );
            }
            return emptyValue;
        }
        return value;
    }


    private List<PolyValue> retrieveTableValue( List<PolyValue> keys ) {
        List<PolyValue> value = null;
        Pair<AlgDataType, Iterable<List<PolyValue>>> pair = reader.getIterableFromQuery( getTableLookupQuery( keys ) );
        for ( List<PolyValue> row : pair.right ) {
            value = row;
            break;
        }
        if ( value == null ) {
            if ( fail ) {
                throw new GenericRuntimeException( "Unable to lookup value for '" + keys + "'" );
            }
            return emptyValue;
        }
        return value;
    }


    private CheckpointQuery getTableLookupQuery( List<PolyValue> keys ) {
        if ( queryBuilder == null ) {
            queryBuilder = DocLookupActivity.getTableLookupQueryBuilder( matchType, keys, rightFields, valueFields );
        }
        Map<Integer, Pair<AlgDataType, PolyValue>> params = new HashMap<>();
        for ( int i = 0; i < keys.size(); i++ ) {
            PolyValue key = keys.get( i );
            params.put( i, Pair.of( factory.createPolyType( key.type ), key ) );
        }
        return queryBuilder.parameters( params ).build();
    }


    private void populateCache( DocReader docReader ) throws Exception {
        if ( docReader.getDocCount() > MAX_CACHE_SIZE ) {
            throw new GenericRuntimeException( "The current implementation only supports lookup collections of size <= " + MAX_CACHE_SIZE );
        }
        boolean isExact = matchType.equals( "EXACT" );
        for ( PolyDocument doc : docReader.getDocIterable() ) {
            List<PolyValue> currentKeys = new ArrayList<>();
            for ( String field : rightFields ) {
                currentKeys.add( ActivityUtils.getSubValue( doc, field ) );
            }
            List<PolyValue> value = new ArrayList<>();
            for ( String field : valueFields ) {
                PolyValue v = ActivityUtils.valueToString( ActivityUtils.getSubValue( doc, field ) );
                value.add( Objects.requireNonNullElse( v, PolyNull.NULL ) );
            }
            if ( isExact ) {
                lookupCache.put( currentKeys, value );
            } else {
                orderedLookupCache.put( currentKeys.get( 0 ), value );
            }
        }
    }


    private void validate( List<String> left, List<String> right, List<String> values, AlgDataType leftType, AlgDataType rightType, String matchType ) throws ActivityException {
        Optional<String> invalid = left.stream().filter( f -> !leftType.getFieldNames().contains( f ) ).findAny();
        if ( invalid.isPresent() ) {
            throw new InvalidSettingException( "Key column does not exist in input table: " + invalid.get(), "leftFields" );
        }
        if ( rightType != null ) {
            // relational lookup table
            List<String> fields = rightType.getFieldNames();
            invalid = right.stream().filter( f -> !fields.contains( f ) ).findAny();
            if ( invalid.isPresent() ) {
                throw new InvalidSettingException( "Key column does not exist in lookup table: " + invalid.get(), "rightFields" );
            }
            invalid = values.stream().filter( f -> !fields.contains( f ) ).findAny();
            if ( invalid.isPresent() ) {
                throw new InvalidSettingException( "Value column does not exist in lookup table: " + invalid.get(), "valueFields" );
            }
        }

        if ( left.size() != right.size() ) {
            throw new InvalidSettingException( "The same number of key fields must be selected", "rightFields" );
        } else if ( left.isEmpty() ) {
            throw new InvalidSettingException( "At least one key column must be selected.", "leftFields" );
        } else if ( values.isEmpty() ) {
            throw new InvalidSettingException( "At least one value field must be selected.", "valueFields" );
        } else if ( !matchType.equals( "EXACT" ) && left.size() > 1 ) {
            throw new InvalidSettingException( "The match type '" + matchType + "' is not supported with multiple key columns", "leftFields" );
        }
    }


    private AlgDataType getType( List<String> leftFields, List<String> values, AlgDataType inType, AlgDataType rightType, String prefix, boolean keepKeys ) {
        if ( !keepKeys ) {
            for ( String field : leftFields ) {
                inType = ActivityUtils.removeField( inType, field );
            }
        }
        Builder builder = ActivityUtils.getBuilder();
        builder.addAll( inType.getFields() );

        if ( rightType != null && ActivityUtils.getDataModel( rightType ) == DataModel.RELATIONAL ) {
            AlgDataType valueType = ActivityUtils.filterFields( rightType, values, false );
            for ( AlgDataTypeField valueField : valueType.getFields() ) {
                builder.add( prefix + valueField.getName(), null, valueField.getType() );
            }
        } else {
            List<String> outFields = values.stream().map( v -> {
                String[] split = v.split( "\\." );
                return split[split.length - 1];
            } ).toList();
            for ( String field : outFields ) {
                builder.add( prefix + field, null, PolyType.TEXT ).nullable( true );
            }
        }
        return builder.uniquify().build();
    }

}

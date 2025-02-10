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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
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
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.activities.TypePreview.LpgType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.DefaultGroup;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.annotations.Group.Subgroup;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.GroupDef;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringSettingDef.AutoCompleteType;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.QueryUtils;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointQuery;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointQuery.CheckpointQueryBuilder;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.DocReader;
import org.polypheny.db.workflow.engine.storage.writer.DocWriter;

@ActivityDefinition(type = "docLookup", displayName = "Document Value Lookup", categories = { ActivityCategory.TRANSFORM, ActivityCategory.DOCUMENT, ActivityCategory.RELATIONAL },
        inPorts = {
                @InPort(type = PortType.DOC, description = "The input collection."),
                @InPort(type = PortType.ANY, description = "The lookup table or collection.") },
        outPorts = { @OutPort(type = PortType.DOC) },
        shortDescription = "Define field(s) in the input collection that are used as key(s) for looking up value(s) in a lookup table or collection."
)
@DefaultGroup(subgroups = {
        @Subgroup(key = "key", displayName = "Key"),
        @Subgroup(key = "value", displayName = "Value")
})

@FieldSelectSetting(key = "leftFields", displayName = "Input Key(s)", simplified = true, reorder = true, targetInput = 0,
        subGroup = "key", pos = 0,
        shortDescription = "Specify the field(s) in the input collection that contain the key to look up.")
@FieldSelectSetting(key = "rightFields", displayName = "Lookup Key(s)", simplified = true, reorder = true, targetInput = 1,
        subGroup = "key", pos = 1,
        shortDescription = "Specify the key field(s) in the lookup input.")

@FieldSelectSetting(key = "valueFields", displayName = "Value Field(s)", simplified = true, reorder = true, targetInput = 1,
        subGroup = "value", pos = 0,
        shortDescription = "Specify the field(s) in the lookup input that contain the value(s) to insert.")
@StringSetting(key = "target", displayName = "Insert Location", defaultValue = "_lookup",
        subGroup = "value", pos = 1,
        autoCompleteType = AutoCompleteType.FIELD_NAMES, autoCompleteInput = 0, maxLength = 1024,
        shortDescription = "Specify the target (sub)field for inserting the lookup value. If empty, the value is inserted with the original field names.")

@BoolSetting(key = "keepKeys", displayName = "Keep Key Field(s)", defaultValue = false,
        group = GroupDef.ADVANCED_GROUP, pos = 0)
@EnumSetting(key = "matchType", displayName = "Match Type", pos = 1,
        options = { "EXACT", "SMALLER", "LARGER" },
        displayOptions = { "Equal", "Next Smaller", "Next Larger" },
        defaultValue = "EXACT", group = GroupDef.ADVANCED_GROUP)
@BoolSetting(key = "fail", displayName = "Fail on Missing Key", defaultValue = false,
        group = GroupDef.ADVANCED_GROUP, pos = 2,
        shortDescription = "If true, attempting to look up a key that does not exist fails the execution.")

@SuppressWarnings("unused")
public class DocLookupActivity implements Activity {

    private static final int MAX_CACHE_SIZE = 100_000;
    private final Map<List<PolyValue>, Map<PolyString, PolyValue>> lookupCache = new HashMap<>();
    private final NavigableMap<PolyValue, Map<PolyString, PolyValue>> orderedLookupCache = new TreeMap<>();
    private final Map<PolyString, PolyValue> emptyValue = new HashMap<>();
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
        /*if ( right instanceof DocType ) {
            String matchType = settings.getString( "matchType" );
            if (matchType != null && !matchType.equals( "EXACT" )) {
                throw new InvalidSettingException( "Collection lookups currently do not support the match type " + matchType, "matchType" );
            }
        }*/

        Set<String> fields = new HashSet<>();
        if ( left instanceof DocType doc ) {
            fields.addAll( doc.getKnownFields() );
        }

        if ( settings.keysPresent( "leftFields", "keepKeys", "rightFields", "valueFields", "matchType" ) ) {
            List<String> leftFields = settings.getOrThrow( "leftFields", FieldSelectValue.class ).getInclude();
            boolean keepKeys = settings.getBool( "keepKeys" );
            List<String> rightFields = settings.getOrThrow( "rightFields", FieldSelectValue.class ).getInclude();
            List<String> values = settings.getOrThrow( "valueFields", FieldSelectValue.class ).getInclude();

            AlgDataType relType = right.getDataModel() == DataModel.RELATIONAL ? right.getNullableType() : null;
            validate( leftFields, rightFields, values, relType, settings.getString( "matchType" ) );

            String target = settings.getNullableString( "target" );
            if ( target != null ) {
                if ( !target.isEmpty() ) {
                    fields.add( target.split( "\\.", 2 )[0] );
                }
            } else {
                List<String> outFields = values.stream().map( v -> {
                    String[] split = v.split( "\\." );
                    return split[split.length - 1];
                } ).toList();
                fields.addAll( outFields );
            }
            if ( !keepKeys ) {
                leftFields.stream().filter( v -> !v.contains( "." ) ).toList().forEach( fields::remove );
            }
        }

        return DocType.of( fields ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        reader = inputs.get( 1 );
        boolean isCollection = reader.getDataModel() == DataModel.DOCUMENT;
        matchType = settings.getString( "matchType" );
        leftFields = settings.get( "leftFields", FieldSelectValue.class ).getInclude();
        rightFields = settings.get( "rightFields", FieldSelectValue.class ).getInclude();
        valueFields = settings.get( "valueFields", FieldSelectValue.class ).getInclude();
        emptyValue.putAll( valueFields.stream().collect( Collectors.toMap( PolyString::of, s -> PolyNull.NULL ) ) );
        fail = settings.getBool( "fail" );
        boolean keepKeys = settings.getBool( "keepKeys" );
        String target = settings.getString( "target" );
        boolean hasTarget = target != null && !target.isBlank();
        boolean singleValue = valueFields.size() == 1;

        if ( isCollection ) {
            populateCache( (DocReader) reader );
        }

        DocWriter writer = ctx.createDocWriter( 0 );
        long inCount = inputs.get( 0 ).getTupleCount();
        long countDelta = Math.max( inCount / 100, 1 );
        long count = 0;
        for ( PolyDocument doc : ((DocReader) inputs.get( 0 )).getDocIterable() ) {
            List<PolyValue> keys = getKeys( doc );

            Map<PolyString, PolyValue> values = isCollection ? lookupCollection( keys ) : lookupTable( keys );

            if ( !keepKeys ) {
                for ( String key : leftFields ) {
                    ActivityUtils.removeSubValue( doc, key );
                }
            }
            if ( hasTarget ) {
                PolyValue toInsert = singleValue ? values.values().iterator().next() : PolyDocument.ofDocument( values );
                System.out.println( "Value to insert: " + toInsert );
                ActivityUtils.insertSubValue( doc, target, toInsert );
            } else {
                doc.putAll( values );
            }

            writer.write( doc );
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


    private List<PolyValue> getKeys( PolyDocument doc ) throws Exception {
        List<PolyValue> keys = new ArrayList<>();
        for ( String path : leftFields ) {
            keys.add( ActivityUtils.getSubValue( doc, path ) );
        }
        return keys;
    }


    private Map<PolyString, PolyValue> lookupTable( List<PolyValue> keys ) throws Exception {
        if ( lookupCache.containsKey( keys ) ) {
            return lookupCache.get( keys );
        } else {
            Map<PolyString, PolyValue> value = retrieveTableValue( keys );
            if ( lookupCache.size() > MAX_CACHE_SIZE ) {
                lookupCache.remove( lookupCache.keySet().iterator().next() );
            }
            lookupCache.put( keys, value );
            return value;
        }
    }


    private Map<PolyString, PolyValue> lookupCollection( List<PolyValue> keys ) {
        Map<PolyString, PolyValue> value = switch ( matchType ) {
            case "EXACT" -> {
                Map<PolyString, PolyValue> v = lookupCache.get( keys );
                yield v == null ? Map.of() : v;
            }
            case "SMALLER" -> {
                Entry<PolyValue, Map<PolyString, PolyValue>> entry = orderedLookupCache.floorEntry( keys.get( 0 ) );
                yield entry == null ? Map.of() : entry.getValue();
            }
            case "LARGER" -> {
                Entry<PolyValue, Map<PolyString, PolyValue>> entry = orderedLookupCache.ceilingEntry( keys.get( 0 ) );
                yield entry == null ? Map.of() : entry.getValue();
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


    private Map<PolyString, PolyValue> retrieveTableValue( List<PolyValue> keys ) {
        Map<PolyString, PolyValue> value = new HashMap<>();
        Pair<AlgDataType, Iterable<List<PolyValue>>> pair = reader.getIterableFromQuery( getTableLookupQuery( keys ) );
        for ( List<PolyValue> row : pair.right ) {
            for ( int i = 0; i < row.size(); i++ ) {
                value.put( PolyString.of( valueFields.get( i ) ), row.get( i ) );
            }
            break;
        }
        if ( value.isEmpty() ) {
            if ( fail ) {
                throw new GenericRuntimeException( "Unable to lookup value for '" + keys + "'" );
            }
            return emptyValue;
        }
        return value;
    }


    private CheckpointQuery getTableLookupQuery( List<PolyValue> keys ) {
        if ( queryBuilder == null ) {
            switch ( matchType ) {
                case "EXACT" -> {
                    List<String> conditions = rightFields.stream().map( f -> QueryUtils.quote( f ) + " = ?" ).toList();
                    String query = "SELECT " + QueryUtils.quoteAndJoin( valueFields ) + " FROM " + CheckpointQuery.ENTITY() +
                            " WHERE " + String.join( " AND ", conditions ) +
                            " LIMIT 1";
                    System.out.println( "EXACT Query: " + query );
                    queryBuilder = CheckpointQuery.builder()
                            .query( query )
                            .queryLanguage( "SQL" );
                }
                case "SMALLER" -> {
                    String field = QueryUtils.quote( rightFields.get( 0 ) );
                    String query = "SELECT " + QueryUtils.quoteAndJoin( valueFields ) + " FROM " + CheckpointQuery.ENTITY() +
                            " WHERE " + field + " <= ?" +
                            " ORDER BY " + field + " DESC LIMIT 1";
                    System.out.println( "SMALLER Query: " + query );
                    queryBuilder = CheckpointQuery.builder()
                            .query( query )
                            .queryLanguage( "SQL" );
                }
                case "LARGER" -> {
                    String field = QueryUtils.quote( rightFields.get( 0 ) );
                    String query = "SELECT " + QueryUtils.quoteAndJoin( valueFields ) + " FROM " + CheckpointQuery.ENTITY() +
                            " WHERE " + field + " >= ?" +
                            " ORDER BY " + field + " ASC LIMIT 1";
                    System.out.println( "LARGER Query: " + query );
                    queryBuilder = CheckpointQuery.builder()
                            .query( query )
                            .queryLanguage( "SQL" );
                }
                default -> throw new IllegalStateException( "Unexpected value: " + matchType );
            }
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
            Map<PolyString, PolyValue> value = new HashMap<>();
            for ( String field : valueFields ) {
                PolyValue subValue = ActivityUtils.getSubValue( doc, field );
                value.put( PolyString.of( field ), Objects.requireNonNullElse( subValue, PolyNull.NULL ) );
            }
            if ( isExact ) {
                lookupCache.put( currentKeys, value );
            } else {
                orderedLookupCache.put( currentKeys.get( 0 ), value );
            }
        }
    }


    private void validate( List<String> left, List<String> right, List<String> values, AlgDataType rightType, String matchType ) throws ActivityException {
        if ( rightType != null ) {
            // relational lookup table
            List<String> fields = rightType.getFieldNames();
            Optional<String> invalid = right.stream().filter( f -> !fields.contains( f ) ).findAny();
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
            throw new InvalidSettingException( "At least one key field must be selected.", "leftFields" );
        } else if ( values.isEmpty() ) {
            throw new InvalidSettingException( "At least one value field must be selected.", "valueFields" );
        } else if ( !matchType.equals( "EXACT" ) && left.size() > 1 ) {
            throw new InvalidSettingException( "The match type '" + matchType + "' is not supported with multiple key fields", "leftFields" );
        }
    }

}

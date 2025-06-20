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

package org.polypheny.db.workflow.dag.activities;

import static com.opencsv.enums.CSVReaderNullFieldIndicator.EMPTY_SEPARATORS;
import static com.opencsv.enums.CSVReaderNullFieldIndicator.NEITHER;

import com.fasterxml.jackson.core.JsonPointer;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.functions.TemporalFunctions;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyTemporal;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.GraphPropertyHolder;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.util.BsonUtil;
import org.polypheny.db.util.Source;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidInputException;
import org.polypheny.db.workflow.engine.storage.StorageManager;

public class ActivityUtils {

    private static final AlgDataTypeFactory factory = AlgDataTypeFactory.DEFAULT;
    private static final Pattern nameValidator = Pattern.compile( "^[a-zA-Z_][a-zA-Z0-9_]*$" ); // for entities / columns / fields etc.
    private static final int MAX_NAME_LENGTH = 128;


    public static DataModel getDataModel( AlgDataType type ) {
        return switch ( type.getPolyType() ) {
            case DOCUMENT -> DataModel.DOCUMENT;
            case GRAPH -> DataModel.GRAPH;
            default -> DataModel.RELATIONAL;
        };
    }


    public static boolean hasRequiredFields( AlgDataType type ) {
        if ( getDataModel( type ) != DataModel.RELATIONAL ) {
            return true;
        }
        return StorageManager.isPkCol( type.getFields().get( 0 ) );
    }


    public static Builder getBuilder() {
        return factory.builder();
    }


    public static AlgDataType addPkCol( AlgDataType type ) {
        Builder builder = factory.builder();
        builder.add( StorageManager.PK_COL, null, PolyType.BIGINT );
        for ( AlgDataTypeField field : type.getFields() ) {
            builder.add( field );
        }
        return builder.uniquify().build();
    }


    public static AlgNode addPkCol( AlgNode input, AlgCluster cluster ) {
        List<RexNode> projects = new ArrayList<>();
        AlgDataType type = input.getTupleType();
        projects.add( cluster.getRexBuilder().makeBigintLiteral( new BigDecimal( 0 ) ) ); // Add new PK col
        IntStream.range( 0, type.getFieldCount() )
                .mapToObj( i -> new RexIndexRef( i, type.getFields().get( i ).getType() ) )
                .forEach( projects::add );
        return LogicalRelProject.create( input, projects, addPkCol( type ) );
    }


    public static AlgNode removePkCol( AlgNode input, AlgCluster cluster ) {
        List<RexNode> projects = new ArrayList<>();
        AlgDataType type = input.getTupleType();
        IntStream.range( 1, type.getFieldCount() )
                .mapToObj( i -> new RexIndexRef( i, type.getFields().get( i ).getType() ) )
                .forEach( projects::add );
        return LogicalRelProject.create( input, projects, removeField( type, StorageManager.PK_COL ) );
    }


    /**
     * Reorders the field present in fields according to the order in the list. Duplicates are removed. Fields not in the list are excluded.
     * If ensureFirstIsPk is true, it is assumed that type has the PK_COL field, but it might not be present in fields or at the wrong position.
     * In this case, it will be added.
     */
    public static AlgDataType filterFields( AlgDataType type, List<String> fields, boolean ensureFirstIsPk ) {
        List<String> include = new ArrayList<>( fields.stream().distinct().toList() );
        if ( ensureFirstIsPk && (fields.isEmpty() || !fields.get( 0 ).equals( StorageManager.PK_COL )) ) {
            include.remove( StorageManager.PK_COL ); // remove if not at first index
            include.add( 0, StorageManager.PK_COL );
        }
        Builder builder = factory.builder();
        for ( String name : include ) {
            AlgDataTypeField field = type.getField( name, true, false );
            if ( field != null ) {
                builder.add( field );
            }
        }
        return builder.build();
    }


    /**
     * Renames the fields of the input type (must have PK_COL as first column) using the given mapping.
     * PK_COL cannot be renamed. The resulting type is guaranteed to have unique field names.
     */
    public static AlgDataType renameFields( AlgDataType type, Map<String, String> mapping ) {
        Builder builder = factory.builder();
        for ( AlgDataTypeField field : type.getFields() ) {
            String name = field.getName();
            if ( !name.equals( StorageManager.PK_COL ) && mapping.containsKey( name ) ) {
                name = mapping.get( name );
            }
            builder.add( name, null, field.getType() );
        }
        builder.uniquify();
        return builder.build();
    }


    /**
     * Adds the specified field to the given struct type and uniquifies all fields.
     */
    public static AlgDataType appendField( AlgDataType type, String fieldName, AlgDataType fieldType ) {
        return factory.builder()
                .addAll( type.getFields() )
                .add( fieldName, null, fieldType )
                .uniquify()
                .build();
    }


    public static AlgDataType removeField( AlgDataType type, String fieldName ) {
        Builder builder = factory.builder();
        for ( AlgDataTypeField field : type.getFields() ) {
            if ( !field.getName().equals( fieldName ) ) {
                builder.add( field );
            }
        }
        return builder.build();
    }


    public static AlgDataType removeFields( AlgDataType type, Collection<String> fieldNames ) {
        Builder builder = factory.builder();
        for ( AlgDataTypeField field : type.getFields() ) {
            if ( !fieldNames.contains( field.getName() ) ) {
                builder.add( field );
            }
        }
        return builder.build();
    }


    /**
     * Concatenates the specified types and uniquifies them.
     */
    public static AlgDataType concatTypes( AlgDataType first, AlgDataType second ) {
        return factory.builder()
                .addAll( first.getFields() )
                .addAll( second.getFields() )
                .uniquify()
                .build();
    }


    public static AlgDataType mergeTypesOrThrow( List<AlgDataType> types ) throws InvalidInputException {
        // all inputs must not be null!
        AlgDataType type = AlgDataTypeFactory.DEFAULT.leastRestrictive( types );

        if ( type == null ) {
            throw new InvalidInputException( "The tuple types of the inputs are incompatible", 1 );
        }
        return type;
    }


    public static boolean areTypesCompatible( List<AlgDataType> types ) {
        AlgDataType type = AlgDataTypeFactory.DEFAULT.leastRestrictive( types );
        if ( type == null ) {
            return false;
        }
        for ( AlgDataTypeField field : type.getFields() ) {
            if ( field.getType() == null ) {
                return false;
            }
        }
        return true;
    }


    public static PolyString valueToPolyString( PolyValue value ) {
        return switch ( value.getType() ) {
            case NULL -> PolyString.of( null );
            case ANY, DOCUMENT, GRAPH, NODE, EDGE, PATH, ARRAY, MAP -> value.toPolyJson();
            default -> PolyString.convert( value );
        };
    }


    public static String valueToString( PolyValue value ) {
        return valueToPolyString( value ).value;
    }


    public static PolyValue stringToPolyValue( String value, PolyType type ) {
        if ( value == null ) {
            return PolyNull.NULL;
        }
        if ( type.getFamily() == PolyTypeFamily.CHARACTER ) { // -> string casts succeed more often
            return PolyString.of( value );
        } else if ( value.isEmpty() ) {
            return PolyNull.NULL;
        } else if ( type == PolyType.BOOLEAN ) { // PolyString -> PolyBoolean conversion is not directly supported by the converter
            String str = value.trim().toLowerCase( Locale.ROOT );
            return PolyBoolean.of( str.equals( "true" ) || str.equals( "1" ) );
        }
        return PolyValue.getConverter( type ).apply( PolyString.of( value.trim() ) );
    }


    /**
     * unlike polyValue.toJson(), this always generates valid JSON
     */
    public static String valueToJson( PolyValue value ) {
        if ( value == null ) {
            return "null";
        } else if ( value.isString() ) {
            return value.asString().toQuotedJson();
        }
        return value.toJson();
    }


    /**
     * A more forgiving version of PolyValue.getConverter(value).apply(value)
     */
    public static PolyValue castPolyValue( PolyValue value, PolyType type ) {
        if ( value == null ) {
            return PolyNull.NULL;
        }

        if ( value.type == type ) {
            return value;
        }

        if ( !value.isNull() && value.isList() ) {
            if ( type != PolyType.ARRAY ) {
                // type is inner type
                return PolyList.of( value.asList().stream().map( e -> castPolyValue( e, type ) ).toList() );
            }
            return value;
        }
        if ( type.getFamily() == PolyTypeFamily.CHARACTER ) { // -> string casts succeed more often
            return ActivityUtils.valueToPolyString( value );
        } else if ( value.isString() && value.asString().value.isEmpty() ) {
            return PolyNull.NULL;
        } else if ( type == PolyType.BOOLEAN && value.isString() ) {
            String str = value.asString().value.trim().toLowerCase( Locale.ROOT );
            return PolyBoolean.of( str.equals( "true" ) || str.equals( "1" ) );
        } else if ( value.isString() && PolyType.DATETIME_TYPES.contains( type ) ) {
            return castPolyTemporal( value.asString(), type );
        }
        return PolyValue.getConverter( type ).apply( value );
    }


    public static PolyTemporal castPolyTemporal( PolyString value, PolyType type ) {
        if ( value.value.isEmpty() ) {
            return PolyNull.NULL.asTemporal();
        }
        return switch ( type ) {
            case DATE -> {
                try {
                    yield TemporalFunctions.dateStringToUnixDate( value );
                } catch ( Exception ignored ) {
                }
                try {
                    yield TemporalFunctions.timeStringToUnixDate( value );
                } catch ( Exception ignored ) {
                }
                yield TemporalFunctions.timestampStringToUnixDate( value );
            }
            case TIME -> TemporalFunctions.toTimeWithLocalTimeZone( value );
            case TIMESTAMP -> TemporalFunctions.toTimestampWithLocalTimeZone( value );
            default -> throw new IllegalArgumentException( "Unsupported type: " + type );
        };
    }


    /**
     * Converts a PolyDocument into a PolyDict to be used as graph properties.
     * Nested documents get stringified as JSON and keys with null values are omitted (not supported by graph model).
     */
    public static PolyDictionary docToDict( PolyDocument doc ) {
        PolyDictionary dict = new PolyDictionary();
        for ( Entry<PolyString, PolyValue> entry : doc.entrySet() ) {
            PolyValue value = entry.getValue();
            if ( value == null || value.isNull() ) {
                continue;
            }
            if ( value.isList() ) {
                PolyList<PolyValue> list = value.asList();
                for ( int i = 0; i < list.size(); i++ ) {
                    PolyValue item = list.get( i );
                    if ( item instanceof PolyDocument ) {
                        list.set( i, item.toPolyJson() );
                    }
                }
            } else if ( value.isDocument() ) {
                value = value.toPolyJson();
            }
            dict.put( entry.getKey(), value );
        }
        return dict;
    }


    public static List<Integer> getRegexMatchPositions( String regex, List<String> candidates ) {
        return getRegexMatchPositions( Pattern.compile( regex ), candidates );
    }


    public static List<Integer> getRegexMatchPositions( Pattern pattern, List<String> candidates ) {
        List<Integer> matches = new ArrayList<>();
        for ( int i = 0; i < candidates.size(); i++ ) {
            if ( pattern.matcher( candidates.get( i ) ).matches() ) {
                matches.add( i );
            }
        }
        return Collections.unmodifiableList( matches );
    }


    public static List<String> getRegexMatches( String regex, List<String> candidates ) {
        return getRegexMatches( Pattern.compile( regex ), candidates );
    }


    public static List<String> getRegexMatches( Pattern pattern, List<String> candidates ) {
        return candidates.stream().filter( pattern.asMatchPredicate() ).toList();
    }


    public static RexLiteral getRexLiteral( int i ) {
        PolyValue value = PolyInteger.of( i );
        return new RexLiteral( value, factory.createPolyType( value.type ), value.type );
    }


    public static RexLiteral getRexLiteral( String s ) {
        PolyValue value = PolyString.of( s );
        return new RexLiteral( value, factory.createPolyType( value.type ), value.type );
    }


    public static RexLiteral getRexLiteral( PolyValue value, AlgDataType type ) {
        return new RexLiteral( value, type, value.type );
    }


    public static RexNameRef getDocRexNameRef( String target, @Nullable Integer inputIndex ) {
        return RexNameRef.create( List.of( target.split( "\\." ) ), inputIndex, DocumentType.ofId() );
    }


    /**
     * Returns a AlgDataType representing the input type with any quotes in field names removed.
     * The resulting columns might no longer be unique.
     */
    public static AlgDataType removeQuotesInNames( AlgDataType type ) {
        Builder builder = factory.builder();
        for ( AlgDataTypeField field : type.getFields() ) {
            builder.add( field.getName().replace( "\"", "" ), null, field.getType() );
        }
        return builder.build();
    }


    public static List<String> removeQuotesInNames( List<String> names ) {
        return names.stream().map( name -> name.replace( "\"", "" ) ).toList();
    }


    public static Optional<String> findInvalidFieldName( List<String> names ) {
        for ( String name : names ) {
            if ( isInvalidFieldName( name ) ) {
                return Optional.of( name );
            }
        }
        return Optional.empty();
    }


    public static boolean isInvalidFieldName( String name ) {
        return name.length() > MAX_NAME_LENGTH || !nameValidator.matcher( name ).matches();
    }


    public static void validateFieldNames( List<String> names ) throws GenericRuntimeException {
        Optional<String> invalid = ActivityUtils.findInvalidFieldName( names );
        if ( invalid.isPresent() ) {
            throw new GenericRuntimeException( "Invalid column name: " + invalid.get() );
        }
    }


    /**
     * Resolve the subfield pointer (subfields specified with '.').
     *
     * @param doc root document
     * @param pointer the pointer
     * @return the PolyValue at the specified pointer or null if the value is unset.
     * @throws Exception if the pointer points to a location that does not exist or the structure is inconsistent with the pointer.
     */
    public static PolyValue getSubValue( PolyDocument doc, String pointer ) throws Exception {
        if ( pointer.isEmpty() ) {
            return doc;
        }
        PolyValue current = doc;
        for ( String s : pointer.split( "\\." ) ) {
            current = switch ( current.getType() ) {
                case DOCUMENT -> {
                    PolyString next = PolyString.of( s );
                    yield current.asDocument().get( next );
                }
                case ARRAY -> {
                    int next = Integer.parseInt( s );
                    yield current.asList().get( next );
                }
                default -> throw new IllegalStateException( "Unexpected type: " + current.getType() );
            };
        }
        return current;
    }


    /**
     * Inserts the specified replacement value into the document (in-place operation).
     * If the pointer points to an array element that does not exist yet, a new element is added
     * (resulting in the specified index possibly being larger than the actual index after insertion).
     *
     * @param doc the document to modify
     * @param pointer a pointer to the insertion location. The entire path except for the last key or index must exist.
     * @param replacement the value to either replace an existing value or get inserted as a new value
     * @throws Exception If the path is invalid or the replacement cannot be inserted at that location
     */
    public static void insertSubValue( PolyDocument doc, String pointer, PolyValue replacement ) throws Exception {
        if ( pointer.isEmpty() ) {
            throw new IllegalArgumentException( "Pointer must not be empty" );
        }

        int lastDotIndex = pointer.lastIndexOf( '.' );
        String path = "";
        String key;
        if ( lastDotIndex == -1 ) {
            key = pointer;
        } else {
            path = pointer.substring( 0, lastDotIndex );
            key = pointer.substring( lastDotIndex + 1 );
            if ( key.isBlank() ) {
                throw new IllegalArgumentException( "Invalid Pointer: " + pointer );
            }
        }

        PolyValue target = getSubValue( doc, path ); // target is either a PolyDocument or PolyList
        if ( target.isDocument() ) {
            target.asDocument().put( PolyString.of( key ), replacement );
        } else if ( target.isList() ) {
            int i = Integer.parseInt( key );
            if ( i >= target.asList().size() ) {
                target.asList().add( replacement );
            } else {
                target.asList().set( i, replacement );
            }
        } else {
            throw new IllegalArgumentException( "Target value does not support inserting values: " + path );
        }
    }


    public static boolean hasSubValue( PolyDocument doc, String pointer ) {
        try {
            return getSubValue( doc, pointer ) != null; // PolyNull would evaluate to true
        } catch ( Exception e ) {
            return false;
        }
    }


    /**
     * Removes the value at the specified pointer location from the document.
     *
     * @param doc the document to modify
     * @param pointer a pointer to the field to remove. The entire path except for the last key or index must exist.
     * @throws Exception If the path is invalid
     */
    public static PolyValue removeSubValue( PolyDocument doc, String pointer ) throws Exception {
        if ( pointer.isEmpty() ) {
            throw new IllegalArgumentException( "Pointer must not be empty" );
        }

        int lastDotIndex = pointer.lastIndexOf( '.' );
        String path = "";
        String key;
        if ( lastDotIndex == -1 ) {
            key = pointer;
        } else {
            path = pointer.substring( 0, lastDotIndex );
            key = pointer.substring( lastDotIndex + 1 );
            if ( key.isBlank() ) {
                throw new IllegalArgumentException( "Invalid Pointer: " + pointer );
            }
        }

        PolyValue target = getSubValue( doc, path ); // target is either a PolyDocument or PolyList
        if ( target.isDocument() ) {
            return target.asDocument().remove( PolyString.of( key ) );
        } else if ( target.isList() ) {
            int i = Integer.parseInt( key );
            return target.asList().remove( i );
        } else {
            throw new IllegalArgumentException( "Target value does not support removal: " + path );
        }
    }


    public static String getParentPointer( String pointer ) {
        if ( pointer.isEmpty() ) {
            throw new IllegalArgumentException( "Pointer must not be empty" );
        }
        int lastDotIndex = pointer.lastIndexOf( '.' );
        if ( lastDotIndex == -1 ) {
            return ""; // root
        }
        return pointer.substring( 0, lastDotIndex );
    }


    public static String getChildPointer( String pointer ) {
        if ( pointer.isEmpty() ) {
            throw new IllegalArgumentException( "Pointer must not be empty" );
        }
        if ( pointer.endsWith( "." ) ) {
            throw new IllegalArgumentException( "Pointer must not end with '.'" );
        }
        int lastDotIndex = pointer.lastIndexOf( '.' );
        if ( lastDotIndex == -1 ) {
            return pointer;
        }
        return pointer.substring( lastDotIndex + 1 );
    }


    /**
     * Converts "field.subfield.0.value" to "/field/subfield/0/value"
     */
    public static JsonPointer dotToJsonPointer( String dotPointer ) {
        String jsonPointer = "";
        if ( !dotPointer.isBlank() ) {
            jsonPointer = "/" + dotPointer
                    .replace( "~", "~0" )
                    .replace( "/", "~1" )
                    .replace( ".", "/" );
        }
        return JsonPointer.compile( jsonPointer );
    }


    /**
     * Adds a generated document id to the given map if it does not yet contain a valid id.
     */
    public static void addDocId( Map<PolyString, PolyValue> map ) {
        if ( map.containsKey( Activity.docId ) && map.get( Activity.docId ).isString() ) {
            return;
        }
        map.put( Activity.docId, PolyString.of( BsonUtil.getObjectId() ) );
    }


    public static boolean isAtomicValue( PolyValue subValue ) {
        return subValue == null || subValue.isNull() || !(subValue.isList() || subValue.isDocument() || subValue.isMap());
    }


    /**
     * @return true if holder has a label that is in the labels collection or labels is empty.
     */
    public static boolean matchesLabelList( GraphPropertyHolder holder, Collection<String> labels ) {
        return labels.isEmpty() || holder.getLabels().stream().anyMatch( l -> labels.contains( l.value ) );
    }


    public static CSVReader openCSVReader( Reader reader, char sep, char quote, char escape, int skipLines, boolean emptyFieldIsNull ) {
        CSVParser csvParser = new CSVParserBuilder()
                .withSeparator( sep )
                .withQuoteChar( quote )
                .withEscapeChar( escape )
                .withFieldAsNull( emptyFieldIsNull ? EMPTY_SEPARATORS : NEITHER )
                .withErrorLocale( Locale.getDefault() )
                .build();
        return new CSVReaderBuilder( reader )
                .withCSVParser( csvParser )
                .withSkipLines( skipLines )
                .build();
    }


    public static String resourceNameFromSource( Source source ) throws URISyntaxException {
        return source.protocol().equals( "file" ) ?
                source.file().getName() :
                Paths.get( source.url().toURI().getPath() ).getFileName().toString();
    }


    public static PolyType inferPolyType( String value, PolyType nullType ) {
        if ( value == null ) {
            return nullType;
        }
        value = value.trim();
        if ( value.isEmpty() ) {
            return PolyType.TEXT;
        }
        if ( "true".equalsIgnoreCase( value ) || "false".equalsIgnoreCase( value ) ) {
            return PolyType.BOOLEAN;
        }
        try {
            Long.parseLong( value );
            return PolyType.BIGINT;
        } catch ( NumberFormatException ignored ) {
        }
        try {
            Double.parseDouble( value );
            return PolyType.DOUBLE;
        } catch ( NumberFormatException ignored ) {
        }
        return PolyType.TEXT;
    }


    public static String deriveValidFieldName( @Nullable String field, String prefixIfInvalid, int indexIfInvalid ) {
        if ( field == null ) {
            return prefixIfInvalid + indexIfInvalid;
        }
        field = field.trim().replace( " ", "_" );
        field = Normalizer.normalize( field, Normalizer.Form.NFD ); // map ä to a
        field = field.replaceAll( "[^a-zA-Z0-9_]", "" ).toLowerCase( Locale.ROOT );
        if ( field.isBlank() ) {
            return prefixIfInvalid + indexIfInvalid;
        }
        if ( isInvalidFieldName( field ) ) {
            return prefixIfInvalid + field;
        }
        return field;
    }


    /**
     * TuplePipes are a simple way to transform tuples after reading or before writing them to a checkpoint.
     * It is not exclusive to Pipeable activities, but can also be useful in other situations.
     */

    @Getter
    public static abstract class TuplePipe {

        final AlgDataType inType;


        TuplePipe( AlgDataType inType ) {
            this.inType = inType;
        }


        public abstract AlgDataType getOutType();


        abstract List<PolyValue> transform( List<PolyValue> input );


        private Iterator<List<PolyValue>> getIterator( Iterator<List<PolyValue>> input ) {
            return new Iterator<>() {

                @Override
                public boolean hasNext() {
                    return input.hasNext();
                }


                @Override
                public List<PolyValue> next() {
                    return transform( input.next() );
                }
            };
        }


        public final Iterable<List<PolyValue>> pipe( Iterator<List<PolyValue>> input ) {
            return () -> getIterator( input );
        }


        public final Iterable<List<PolyValue>> pipe( Iterable<List<PolyValue>> input ) {
            return () -> getIterator( input.iterator() );
        }

    }


    /**
     * Transforms any relational tuples with some types possibly being unsupported by relational checkpoints
     * into tuples that can be stored in a relational checkpoint.
     */
    public static class AnyToRelPipe extends TuplePipe {

        // in the future, this could become more elaborate
        private static final Map<PolyType, PolyType> TYPE_MAP = Map.of(
                PolyType.ANY, PolyType.TEXT,
                PolyType.DOCUMENT, PolyType.TEXT,
                PolyType.NODE, PolyType.TEXT,
                PolyType.EDGE, PolyType.TEXT,
                PolyType.PATH, PolyType.TEXT
        );
        private final AlgDataType outType;
        private final PolyType[] targetTypes;
        private final int size;


        public AnyToRelPipe( AlgDataType inType ) {
            super( inType );
            if ( !inType.isStruct() ) {
                throw new IllegalArgumentException( "Only structs are currently supported" );
            }
            this.size = inType.getFieldCount();
            this.targetTypes = new PolyType[size];
            Builder builder = factory.builder();
            for ( int i = 0; i < size; i++ ) {
                AlgDataTypeField field = inType.getFields().get( i );
                PolyType targetType = TYPE_MAP.getOrDefault( field.getType().getPolyType(), null );
                targetTypes[i] = targetType;
                if ( targetType == null ) {
                    builder.add( field );
                } else {
                    builder.add( field.getName(), field.getPhysicalName(), targetType );
                }
            }
            outType = builder.build();
        }


        @Override
        public AlgDataType getOutType() {
            return outType;
        }


        @Override
        List<PolyValue> transform( List<PolyValue> input ) {
            List<PolyValue> output = new ArrayList<>( size );
            for ( int i = 0; i < size; i++ ) {
                PolyValue value = input.get( i );
                PolyType target = targetTypes[i];
                if ( target != null ) {
                    if ( target == PolyType.TEXT ) {
                        value = valueToPolyString( value );
                    } else {
                        throw new NotImplementedException( "Target type " + target + " is not yet implemented" );
                    }
                }
                output.add( value );
            }
            return output;
        }

    }

}

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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import lombok.Getter;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.util.BsonUtil;
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


    public static PolyString valueToString( PolyValue value ) {
        return switch ( value.getType() ) {
            case NULL -> PolyString.of( null );
            case ANY, DOCUMENT, GRAPH, NODE, EDGE, PATH -> value.toPolyJson();
            default -> PolyString.convert( value );
        };
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
        return names.stream().filter( nameValidator.asMatchPredicate().negate() ).filter( s -> s.length() > MAX_NAME_LENGTH ).findFirst();
    }


    public static boolean isValidFieldName( String name ) {
        return name.length() <= MAX_NAME_LENGTH && nameValidator.matcher( name ).matches();
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
     * Adds a generated document id to the given map if it does not yet contain an id.
     */
    public static void addDocId( Map<PolyString, PolyValue> map ) {
        map.putIfAbsent( Activity.docId, PolyString.of( BsonUtil.getObjectId() ) );
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
                        value = valueToString( value );
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

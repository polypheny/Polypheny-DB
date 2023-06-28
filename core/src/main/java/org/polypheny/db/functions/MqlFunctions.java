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

package org.polypheny.db.functions;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.polypheny.db.schema.document.DocumentUtil;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyInteger;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.util.Pair;


/**
 * Repository class, which defines different functions used, when handling the document model
 */
public class MqlFunctions {


    public static final Gson GSON = new GsonBuilder()
            .enableComplexMapKeySerialization()
            .create();


    private MqlFunctions() {
        // empty on purpose
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyValue docQueryValue( PolyValue input, List<PolyString> filters ) {
        if ( input == null || !input.isDocument() ) {
            return null;
        }
        PolyValue temp = input;
        for ( PolyString filter : filters ) {
            if ( !temp.isDocument() ) {
                return null;
            }
            temp = temp.asDocument().get( filter );
            if ( temp == null ) {
                return null;
            }
        }

        return temp;
    }


    /**
     * Adds a new field to an existing object
     *
     * @param input the document, to which the new field is added
     * @param name the name of the added field
     * @param object th field value, which is added
     * @return the new object, with the value included
     */
    @SuppressWarnings("UnusedDeclaration")
    public static Object docAddFields( Object input, String name, Object object ) {
        if ( input instanceof String ) {
            BsonDocument document = BsonDocument.parse( (String) input );
            document.put( name, DocumentUtil.getBson( object ) );

            return document.toJson();
        }

        return null;
    }


    /**
     * Transformer methode, which outputs a JSON representation of the provided object
     *
     * @param input the untransformed object
     * @return a transformed object as JSON string
     */
    @SuppressWarnings("UnusedDeclaration")
    public static Object docJsonify( Object input ) {
        if ( input instanceof BsonDocument ) {
            return ((BsonDocument) input).toJson();
        } else if ( input instanceof Map ) {
            return GSON.toJson( input );
        } else if ( input instanceof List ) {
            return GSON.toJson( input );
        } else {
            return input;
        }
    }


    /**
     * Tries to add the provided value to an array/list,
     * if the object is not in such a form it fails
     *
     * @param input the object, which should be of list/array form
     * @param value the object, which is added.
     * @return a new list, which included the new value
     */
    @SuppressWarnings("UnusedDeclaration")
    public static Object docAddToSet( Object input, Object value ) {
        input = deserializeBsonIfNecessary( input );
        value = deserializeBsonIfNecessary( value );

        if ( input instanceof List ) {
            if ( ((List<Object>) input).contains( value ) ) {
                return input;
            }
            ((List<Object>) input).add( value );
            return input;
        } else {
            throw new RuntimeException( "AddToSet can only be applied to arrays" );
        }
    }


    /**
     * Compares a specified entry in the document with the provided comparator
     * and replaces it with the smaller value
     *
     * @param input the document/object, which entry is compared and replaced
     * @param comparator the value, which is compared with
     * @return a new document/object, which holds the minimal value
     */
    @SuppressWarnings("UnusedDeclaration")
    public static Object docUpdateMin( Object input, Object comparator ) {
        return docUpdateMinMax( input, comparator ) <= 0 ? input : comparator;
    }


    /**
     * Compares a specified entry in the document with the provided comparator
     * and replaces it with the bigger value
     *
     * @param input the document/object, which entry is compared and replaced
     * @param comparator the value, which is compared with
     * @return a new document/object, which holds the maximal value
     */
    @SuppressWarnings("UnusedDeclaration")
    public static Object docUpdateMax( Object input, Object comparator ) {
        return docUpdateMinMax( input, comparator ) <= 0 ? comparator : input;
    }


    /**
     * Comparable method, which handles the comparing of two untyped elements
     *
     * @param input the original value
     * @param comparator the new value
     * @return number identifying, if comparator is bigger ( -1 ) or smaller ( 1 )
     */
    @SuppressWarnings("UnusedDeclaration")
    private static int docUpdateMinMax( Object input, Object comparator ) {
        if ( input instanceof Comparable && comparator instanceof Comparable ) {
            return ((Comparable) input).compareTo( comparator );
        } else {
            throw new RuntimeException( "The provided values where not comparable." );
        }
    }


    /**
     * Update method, which deletes the provided name
     *
     * @param input the full object/document, from which the values are removed
     * @param names the name in a list form <pre>key1.key2.key3 {@code ->} [key1,key2,key3]</pre>
     * @return the object/document, without the filtered name
     */
    @SuppressWarnings("UnusedDeclaration")
    public static Object docUpdateRemove( Object input, List names ) {
        // TODO enable as soon as pushing down of Modify is possible
        Map<String, ?> initial = (Map) deserializeBsonIfNecessary( input );
        Map<String, ?> doc = initial;
        String name;
        Iterator<String> iter = names.iterator();
        while ( iter.hasNext() ) {
            name = iter.next();
            if ( doc.containsKey( name ) ) {
                if ( !iter.hasNext() ) {
                    doc.remove( name );
                } else {
                    if ( doc.get( name ) instanceof Map ) {
                        doc = (Map<String, ?>) doc.get( name );
                    } else {
                        return docJsonify( initial );
                    }

                }
            }
        }

        return docJsonify( initial );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyDocument docUpdateReplace( PolyValue input, List<PolyString> names, List<PolyValue> values ) {
        if ( !input.isDocument() ) {
            return new PolyDocument();
        }
        for ( Pair<PolyString, PolyValue> pair : Pair.zip( names, values ) ) {
            updateValue( pair.right, input.asDocument(), List.of( pair.left.value.split( "\\." ) ) );
        }

        return input.asDocument();
    }


    private static void updateValue( PolyValue value, PolyDocument doc, List<String> splitName ) {
        PolyString name;
        Iterator<PolyString> iter = splitName.stream().map( PolyString::of ).iterator();

        while ( iter.hasNext() ) {
            name = iter.next();
            if ( doc.containsKey( name ) ) {
                if ( !iter.hasNext() ) {
                    doc.put( name, value );
                } else {
                    doc = doc.get( name ).asDocument();
                }
            }
        }
    }


    @SuppressWarnings("UnusedDeclaration")
    public static Object docUpdateRename( Object input, List names, List newNames ) {
        // TODO enable as soon as pushing down of Modify is possible
        Map<String, Object> initial = (Map) deserializeBsonIfNecessary( input );
        Map<String, Object> doc = initial;
        String name;
        int count = -1;
        Iterator<String> iter = names.iterator();
        while ( iter.hasNext() ) {
            name = iter.next();
            if ( doc.containsKey( name ) ) {
                if ( !iter.hasNext() ) {
                    Object obj = doc.get( name );
                    doc.put( String.join( ".", (List) newNames.get( count ) ), obj );
                } else {
                    if ( doc.get( name ) instanceof Map ) {
                        doc = (Map<String, Object>) doc.get( name );
                    } else {
                        return docJsonify( initial );
                    }

                }
            }
        }

        return docJsonify( initial );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyDocument renameDocument( PolyValue doc, PolyString key, PolyString rename ) {
        return new PolyDocument( rename, doc.asDocument().map.get( key ) );
    }


    /**
     * Scans the object/document and removes matching filters
     *
     * @param input the object/document, form which the filters are removed
     * @param excluded multiple filters, group in collections [key1.key2.key3, key1.key2] {@code ->} [[key1, key2, key3],[key1, key2]]
     * @return a filtered object/document
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyValue docQueryExclude( PolyValue input, List<List<String>> excluded ) {
        if ( !(input.isDocument()) ) {
            return null;
        }

        if ( excluded.size() == 0 ) {
            return input;
        }

        excludeBson( input.asDocument(), excluded );
        return input;
    }


    /**
     * Tests if the provided object/document belongs to one of the provided types
     *
     * @param input the object/document, which is tested
     * @param typeNumbers the types as numbers, which return true if matched
     * @return if the object/type is of one of the provided types
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docTypeMatch( PolyValue input, List<PolyInteger> typeNumbers ) {

        if ( input == null ) {
            // if we look for nullType
            return PolyBoolean.of( typeNumbers.contains( PolyInteger.of( 10 ) ) );
        }

        List<Pair<Class<? extends BsonValue>, Class<?>>> clazzPairs = typeNumbers.stream().map( typeNumber -> DocumentUtil.getBsonClass( typeNumber.intValue() ) ).collect( Collectors.toList() );

        return PolyBoolean.of( Pair.right( clazzPairs ).stream().anyMatch( clazz -> clazz.isInstance( input ) ) );
    }


    /**
     * Tests if the object/document matches the provided regex
     *
     * @param input the object/document
     * @param regex the regex to match
     * @param isInsensitive if the matching should be case-insensitive
     * @param isMultiline if multiple lines should be considered
     * @param doesIgnoreWhitespace if whitespace should be ignored
     * @param allowsDot if dots for subfields are allowed
     * @return if the provided object/document conforms to the regex
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docRegexMatch( PolyValue input, PolyString regex, PolyBoolean isInsensitive, PolyBoolean isMultiline, PolyBoolean doesIgnoreWhitespace, PolyBoolean allowsDot ) {
        String adjusted = regex.value;
        if ( input.isString() ) {
            String comp = input.asString().value;
            int flags = 0;
            flags |= Pattern.DOTALL;
            if ( isInsensitive.value ) {
                flags |= Pattern.CASE_INSENSITIVE;
            }
            if ( isMultiline.value ) {
                flags |= Pattern.MULTILINE;
            }
            if ( doesIgnoreWhitespace.value ) {
                adjusted = adjusted.replaceAll( "\\s", "" );
                comp = comp.replaceAll( "\\s", "" );
            }
            if ( allowsDot.value ) {
                flags |= Pattern.DOTALL;
            }

            return PolyBoolean.of( Pattern.compile( ".*" + regex + ".*", flags ).matcher( comp ).matches() );
        }
        return PolyBoolean.FALSE;
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docJsonMatch( PolyValue input, String json ) {
        // TODO use schema validator library
        throw new RuntimeException( "NOT IMPLEMENTED" );
    }


    /**
     * Retrieves and transforms an array from the provided object/document
     *
     * @param input the unparsed and typed array
     * @return the array
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyList<?> docGetArray( PolyValue input ) {
        // input = deserializeBsonIfNecessary( input );
        if ( input.isList() ) {
            return input.asList();
        }
        return PolyList.of();
    }


    /**
     * Retrieves an element in the underlying array
     *
     * @param input the array to scan
     * @param index the element, which is retrieved, negative starts form behind
     * @return the element at the specified position, else null
     */
    @SuppressWarnings("UnusedDeclaration")
    private static PolyValue docItemAny( List<PolyValue> input, int index ) {
        // mongo starts at 0 and allows retrieving from behind with negative
        if ( input.size() > Math.abs( index ) ) {
            if ( index < 0 ) {
                index = input.size() + index;
            }
            return input.get( index );
        }
        return null;
    }


    /**
     * Retrieves an element in the underlying array
     *
     * @param input the array to scan
     * @param index the element, which is retrieved, negative starts form behind
     * @return the element at the specified position, else null
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyValue docItemAny( PolyValue input, Object index ) {
        return docItem( input, ((Number) index).intValue() );
    }


    /**
     * Retrieves an element in the underlying array
     *
     * @param input the array to scan
     * @param index the element, which is retrieved, negative starts form behind
     * @return the element at the specified position, else null
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyValue docItem( PolyValue input, int index ) {
        if ( input.isList() ) {
            return input.asList().get( index );
        }

        return null;
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyDocument mergeDocument( PolyList<PolyString> names, PolyValue... documents ) {
        assert names.size() == documents.length;
        Map<PolyString, PolyValue> doc = new HashMap<>();
        for ( int i = 0; i < documents.length; i++ ) {
            doc.put( names.get( i ), documents[i] );
        }

        return PolyDocument.ofDocument( doc );
    }


    /**
     * If the provided object/document is of type array and matches the specified size
     *
     * @param input the unparsed object/document
     * @param size the size to compare
     * @return if the size matches
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docSizeMatch( PolyValue input, int size ) {
        if ( input.isList() ) {
            return PolyBoolean.of( input.asList().size() == size );
        }
        return PolyBoolean.FALSE;
    }


    /**
     * Special equal operation, which conforms the MongoQl standard.
     * As it is able to deal with null values and matches values compared to arrays correctly
     *
     * @param b0 the left element
     * @param b1 the right element
     * @return if the elements are equal
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docEq( PolyValue b0, PolyValue b1 ) {
        if ( b0 instanceof List && !(b1 instanceof List) ) {
            return PolyBoolean.of( ((List<?>) b0).contains( b1 ) );
        }
        if ( b0 == null || b1 == null ) {
            if ( b0 == null && b1 == null ) {
                return PolyBoolean.TRUE;
            } else {
                return PolyBoolean.FALSE;
            }
        }

        return PolyBoolean.of( b0.compareTo( b1 ) == 0 );
    }


    /**
     * Special greater operation, which conforms the MongoQl standard.
     * As it is able to deal with null values and matches values compared to arrays correctly
     *
     * @param b0 the left element
     * @param b1 the right element
     * @return if the left element is smaller than the right
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docGt( PolyValue b0, PolyValue b1 ) {
        return compNullExecute(
                b0,
                b1,
                () -> Functions.gtAny( b0, b1 ).value );
    }


    /**
     * Special greater than operation, which conforms the MongoQl standard.
     * As it is able to deal with null values and matches values compared to arrays correctly
     *
     * @param b0 the left element
     * @param b1 the right element
     * @return if the left element is smaller equal than the right
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docGte( PolyValue b0, PolyValue b1 ) {
        return compNullExecute(
                b0,
                b1,
                () -> (Functions.gtAny( b0, b1 ).value || Functions.eq( b0, b1 ).value) );
    }


    /**
     * Special less than operation, which conforms the MongoQl standard.
     * As it is able to deal with null values and matches values compared to arrays correctly
     *
     * @param b0 the left element
     * @param b1 the right element
     * @return if the left element is bigger than the right
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docLt( PolyValue b0, PolyValue b1 ) {
        return compNullExecute(
                b0,
                b1,
                () -> Functions.ltAny( b0, b1 ).value );
    }


    /**
     * Special less than equal operation, which conforms the MongoQl standard.
     * As it is able to deal with null values and matches values compared to arrays correctly
     *
     * @param b0 the left element
     * @param b1 the right element
     * @return if the left element is bigger equal than the right
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docLte( PolyValue b0, PolyValue b1 ) {
        return compNullExecute(
                b0,
                b1,
                () -> (Functions.ltAny( b0, b1 ).value || Functions.eqAny( b0, b1 ).value) );
    }


    /**
     * Helper function, which executes different provided suppliers and compares the two elements
     *
     * @param b0 the left element
     * @param b1 the right element
     * @param predicate the predicate, which returns if the condition of left and right match
     * @return if the provided elements fit the provided condition
     */
    @SuppressWarnings("UnusedDeclaration")
    private static PolyBoolean compNullExecute( PolyValue b0, PolyValue b1, Supplier<Boolean> predicate ) {
        if ( b0 == null || b1 == null ) {
            return PolyBoolean.FALSE;
        }
        if ( !(b0 instanceof PolyNumber) || !(b1 instanceof PolyNumber) ) {
            return PolyBoolean.FALSE;
        }
        return PolyBoolean.of( predicate.get() );
    }


    /**
     * Extracts the specified range of elements from a provided array/list.
     * If the provided object/document is not a list/array, null is returned
     *
     * @param input the object/document, which should have type array
     * @param skip the elements to skip
     * @param elements how many elements are returned
     * @return the sliced elements
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyValue docSlice( PolyValue input, int skip, int elements ) {
        if ( !(input.isList()) ) {
            return null;
        } else {
            PolyList<?> list = input.asList();
            // if elements is negative the selection starts from the end
            int end;
            int start;
            if ( elements > 0 ) {
                start = Math.min( skip, list.size() );
                end = Math.min( list.size(), skip + elements );
            } else {
                end = Math.max( 0, list.size() - skip );
                start = Math.max( 0, end + elements );
            }
            return (PolyValue) list.subList( start, end );
        }
    }


    /**
     * Removes the provided filter from the doc
     *
     * @param doc the document to scan
     * @param excluded the element to exclude
     */
    private static void excludeBson( PolyValue doc, List<List<String>> excluded ) {
        if ( doc.isDocument() ) {
            List<String> firsts = excluded.stream().map( e -> e.get( 0 ) ).collect( Collectors.toList() );
            List<PolyString> toRemove = new ArrayList<>();
            doc.asDocument().forEach( ( key, value ) -> {
                int pos = 0;
                List<Integer> matches = new ArrayList<>();
                boolean remove = false;
                for ( String first : firsts ) {
                    if ( key.value.equals( first ) ) {
                        if ( excluded.get( pos ).size() > 1 ) {
                            // the matching goes deeper
                            matches.add( pos );
                        } else {
                            // it an exact match and we exclude all underlying
                            remove = true;
                        }
                    }
                    pos++;
                }
                if ( !remove && matches.size() > 0 ) {
                    excludeBson( value.asDocument(), matches.stream().map( i -> excluded.get( i ).subList( 1, excluded.get( i ).size() ) ).collect( Collectors.toList() ) );
                } else if ( remove ) {
                    toRemove.add( key );
                }
            } );
            toRemove.forEach( r -> doc.asDocument().remove( r ) );
        }
    }


    /**
     * Transforms a provided element into it primitive form if it is not already
     *
     * @param obj the object to transform
     * @return the object as primitive, map or list
     */
    private static Object deserializeBsonIfNecessary( Object obj ) {
        if ( obj instanceof String ) {
            try {
                return transformBsonToPrimitive( BsonDocument.parse( (String) obj ) );
            } catch ( Exception e ) {
                return obj;
            }
        } else if ( obj instanceof BsonValue ) {
            return transformBsonToPrimitive( (BsonValue) obj );
        } else {
            return obj;
        }
    }


    /**
     * Tests if a specified path exists in the provided object/document
     *
     * @param obj the object/document to check
     * @param path the path, which is test in the form key1.key2.key3 {@code ->} [key1. key2, key3]
     * @return if the path exists
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean docExists( PolyValue obj, List<PolyString> path ) {
        if ( obj == null || !obj.isDocument() ) {
            return PolyBoolean.FALSE;
        }
        PolyDocument map = obj.asDocument();
        Iterator<PolyString> iter = path.iterator();
        PolyString current = iter.next();

        while ( map.containsKey( current ) ) {
            obj = map.get( current );
            if ( !iter.hasNext() ) {
                return PolyBoolean.TRUE;
            }
            if ( !(obj instanceof Map) ) {
                return PolyBoolean.FALSE;
            }
            map = map.get( current ).asDocument();
            current = iter.next();
        }

        return PolyBoolean.FALSE;
    }


    /**
     * Transforms a provided Bson object into a primitive from
     *
     * @param doc the document to transform
     * @return the document as primitive
     */
    private static Object transformBsonToPrimitive( BsonValue doc ) {
        if ( doc == null ) {
            return null;
        }
        switch ( doc.getBsonType() ) {
            case INT32:
                return doc.asInt32().getValue();
            case INT64:
                return doc.asInt64().getValue();
            case DOUBLE:
                return doc.asDouble().getValue();
            case STRING:
                return doc.asString().getValue();
            case DECIMAL128:
                return doc.asDecimal128().decimal128Value().bigDecimalValue();
            case DOCUMENT:
                return doc.asDocument().entrySet().stream().collect( Collectors.toMap( Entry::getKey, e -> transformBsonToPrimitive( e.getValue() ) ) );
            case ARRAY:
                return doc.asArray().stream().map( MqlFunctions::transformBsonToPrimitive ).collect( Collectors.toList() );
            default:
                return null;
        }
    }

}

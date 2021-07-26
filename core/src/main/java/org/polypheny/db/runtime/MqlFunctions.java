/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.runtime;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.polypheny.db.document.DocumentTypeUtil;
import org.polypheny.db.util.Pair;

public class MqlFunctions {

    public static Object docQueryValue( Object input, List<String> filters ) {
        input = deserializeBsonIfNecessary( input );
        ArrayList<String> filtersCopy = new ArrayList<>( filters );
        while ( filtersCopy.size() != 0 && input != null ) {
            if ( input instanceof Map ) {
                if ( ((Map<?, ?>) input).containsKey( filtersCopy.get( 0 ) ) ) {
                    input = ((Map<?, ?>) input).get( filtersCopy.get( 0 ) );
                    filtersCopy.remove( 0 );
                } else {
                    input = null;
                }
            } else {
                input = null;
            }
        }
        if ( filtersCopy.size() > 0 ) {
            return null;
        }

        return input;
    }


    public static Collection docUpdate( Collection sink ) {
        return null;
    }


    public static Object docAddFields( Object input, String name, Object object ) {
        if ( input instanceof String ) {
            BsonDocument document = BsonDocument.parse( (String) input );
            document.put( name, DocumentTypeUtil.getBson( object ) );

            return document.toJson();
        }

        return null;
    }


    public static Object docAddToSet( Object input, Object value ) {
        input = deserializeBsonIfNecessary( input );
        value = deserializeBsonIfNecessary( input );

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


    public static Object docUpdateMin( Object input, Object comparator ) {
        return docUpdateMinMax( input, comparator ) <= 0 ? input : comparator;
    }


    public static Object docUpdateMax( Object input, Object comparator ) {
        return docUpdateMinMax( input, comparator ) <= 0 ? comparator : input;
    }


    private static int docUpdateMinMax( Object input, Object comparator ) {
        if ( input instanceof Comparable && comparator instanceof Comparable ) {
            return ((Comparable) input).compareTo( comparator );
        } else {
            throw new RuntimeException( "The provided values where not comparable." );
        }

    }


    public static Object docUpdateRemove( Object input, List names ) {
        return null;
    }


    public static Object docUpdateReplace( Object input, List name, List values ) {
        return null;
    }


    public static Object docUpdateRename( Object input, List oldNames, List newNames ) {
        return null;
    }


    public static BsonValue docQueryExclude( Object input, List<List<String>> excluded ) {
        if ( !(input instanceof String) ) {
            return null;
        }

        BsonValue doc = BsonDocument.parse( (String) input );

        if ( excluded.size() == 0 ) {
            return doc;
        }

        excludeBson( doc, excluded );
        return doc;
    }


    public static BsonValue docQueryExclude( BsonValue input, List<List<String>> excluded ) {
        if ( !input.isDocument() ) {
            return input;
        }

        if ( excluded.size() == 0 ) {
            return input;
        }

        excludeBson( input, excluded );
        return input;
    }


    public static boolean docTypeMatch( Object input, List<Integer> typeNumbers ) {

        if ( input == null ) {
            // if we look for nullType
            return typeNumbers.contains( 10 );
        }

        List<Pair<Class<? extends BsonValue>, Class<?>>> clazzPairs = typeNumbers.stream().map( DocumentTypeUtil::getBsonClass ).collect( Collectors.toList() );

        return Pair.right( clazzPairs ).stream().anyMatch( clazz -> clazz.isInstance( input ) );

    }


    public static boolean docRegexMatch( Object input, String regex, boolean isInsensitive, boolean isMultiline, boolean doesIgnoreWhitespace, boolean allowsDot ) {
        if ( input instanceof String ) {
            int flags = 0;
            if ( isInsensitive ) {
                flags |= Pattern.CASE_INSENSITIVE;
            }
            if ( isMultiline ) {
                flags |= Pattern.MULTILINE;
            }
            if ( doesIgnoreWhitespace ) {
                throw new RuntimeException( "NOT SUPPORTED" );
            }
            if ( allowsDot ) {
                flags |= Pattern.DOTALL;
            }

            return Pattern.compile( regex, flags ).matcher( (String) input ).matches();
        }
        return false;
    }


    public static boolean docJsonMatch( Object input, String json ) {
        // use schema validator library TODO DL
        throw new RuntimeException( "NOT IMPLEMENTED" );
    }


    public static List docGetArray( Object input ) {
        input = deserializeBsonIfNecessary( input );
        if ( input instanceof List ) {
            return (List) input;
        }
        return Collections.emptyList();
    }


    private static Object docItemAny( List input, int index ) {
        // mongo starts at 0 and allows to retrieve from behind with negative
        if ( input.size() > Math.abs( index ) ) {
            if ( index < 0 ) {
                index = input.size() + index;
            }
            return input.get( index );
        }
        return null;
    }


    public static Object docItemAny( Object input, Object index ) {
        if ( index instanceof BigDecimal ) {
            return docItem( input, ((BigDecimal) index).intValue() );
        }
        return docItem( input, (Integer) index );
    }


    public static Object docItem( Object input, int index ) {
        if ( input instanceof String ) {
            BsonDocument doc = BsonDocument.parse( (String) input );
            if ( doc.isArray() ) {
                return docItemAny( doc.asArray(), index );
            }
        } else if ( input instanceof List ) {
            return docItemAny( (List<?>) input, index );
        }

        return null;
    }


    public static Object docItem( BsonArray input, int index ) {
        return docItemAny( input, index );
    }


    public static boolean docSizeMatch( Object input, int size ) {
        if ( input instanceof List ) {
            return ((List<?>) input).size() == size;
        }
        return false;
    }


    public static boolean docEq( Object b0, Object b1 ) {
        if ( b0 instanceof List && !(b1 instanceof List) ) {
            return ((List<?>) b0).contains( b1 );
        }
        if ( b0 == null || b1 == null ) {
            if ( b0 == null && b1 == null ) {
                return true;
            } else {
                return false;
            }
        }

        boolean tes = SqlFunctions.eqAny( b0, b1 );
        return tes;
    }


    public static Object docSlice( Object input, int skip, int elements ) {
        if ( !(input instanceof List) ) {
            return null;
        } else {
            List<?> list = ((List<?>) input);
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
            return list.subList( start, end );

        }
    }


    private static void excludeBson( BsonValue doc, List<List<String>> excluded ) {
        if ( doc.isDocument() ) {
            List<String> firsts = excluded.stream().map( e -> e.get( 0 ) ).collect( Collectors.toList() );
            List<String> toRemove = new ArrayList<>();
            doc.asDocument().forEach( ( key, value ) -> {
                int pos = 0;
                List<Integer> matches = new ArrayList<>();
                boolean remove = false;
                for ( String first : firsts ) {
                    if ( key.equals( first ) ) {
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
                    excludeBson( value, matches.stream().map( i -> excluded.get( i ).subList( 1, excluded.get( i ).size() ) ).collect( Collectors.toList() ) );
                } else if ( remove ) {
                    toRemove.add( key );
                }
            } );
            toRemove.forEach( r -> doc.asDocument().remove( r ) );
        }
    }


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


    private static Object transformBsonToPrimitive( BsonValue doc ) {
        if ( doc == null ) {
            return null;
        }
        switch ( doc.getBsonType() ) {
            case INT32:
                return doc.asInt32().getValue();
            case INT64:
                return doc.asInt64().getValue();
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

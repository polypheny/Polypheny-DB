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

package org.polypheny.db.core;

import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.base.Utf8;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import org.apache.calcite.avatica.util.ByteString;
import org.polypheny.db.util.ConversionUtil;
import org.polypheny.db.util.SaffronProperties;
import org.polypheny.db.util.Util;

public class CoreUtil {

    public static final Suggester EXPR_SUGGESTER = ( original, attempt, size ) -> Util.first( original, "EXPR$" ) + attempt;
    public static final Suggester F_SUGGESTER = ( original, attempt, size ) -> Util.first( original, "$f" ) + Math.max( size, attempt );
    public static final Suggester ATTEMPT_SUGGESTER = ( original, attempt, size ) -> Util.first( original, "$" ) + attempt;


    /**
     * Extracts the values from a collation name.
     *
     * Collation names are on the form <i>charset$locale$strength</i>.
     *
     * @param in The collation name
     * @return A {@link ParsedCollation}
     */
    public static ParsedCollation parseCollation( String in ) {
        StringTokenizer st = new StringTokenizer( in, "$" );
        String charsetStr = st.nextToken();
        String localeStr = st.nextToken();
        String strength;
        if ( st.countTokens() > 0 ) {
            strength = st.nextToken();
        } else {
            strength = SaffronProperties.INSTANCE.defaultCollationStrength().get();
        }

        Charset charset = Charset.forName( charsetStr );
        String[] localeParts = localeStr.split( "_" );
        Locale locale;
        if ( 1 == localeParts.length ) {
            locale = new Locale( localeParts[0] );
        } else if ( 2 == localeParts.length ) {
            locale = new Locale( localeParts[0], localeParts[1] );
        } else if ( 3 == localeParts.length ) {
            locale = new Locale( localeParts[0], localeParts[1], localeParts[2] );
        } else {
            throw RESOURCE.illegalLocaleFormat( localeStr ).ex();
        }
        return new ParsedCollation( charset, locale, strength );
    }


    /**
     * Translates a character set name from a SQL-level name into a Java-level name.
     *
     * @param name SQL-level name
     * @return Java-level name, or null if SQL-level name is unknown
     */
    public static String translateCharacterSetName( String name ) {
        switch ( name ) {
            case "BIG5":
                return "Big5";
            case "LATIN1":
                return "ISO-8859-1";
            case "GB2312":
            case "GBK":
                return name;
            case "UTF8":
                return "UTF-8";
            case "UTF16":
                return ConversionUtil.NATIVE_UTF16_CHARSET_NAME;
            case "UTF-16BE":
            case "UTF-16LE":
            case "ISO-8859-1":
            case "UTF-8":
                return name;
            default:
                return null;
        }
    }


    /**
     * Returns the Java-level {@link Charset} based on given SQL-level name.
     *
     * @param charsetName Sql charset name, must not be null.
     * @return charset, or default charset if charsetName is null.
     * @throws UnsupportedCharsetException If no support for the named charset is available in this instance of the Java virtual machine
     */
    public static Charset getCharset( String charsetName ) {
        assert charsetName != null;
        charsetName = charsetName.toUpperCase( Locale.ROOT );
        String javaCharsetName = translateCharacterSetName( charsetName );
        if ( javaCharsetName == null ) {
            throw new UnsupportedCharsetException( charsetName );
        }
        return Charset.forName( javaCharsetName );
    }


    /**
     * Validate if value can be decoded by given charset.
     *
     * @param value nls string in byte array
     * @param charset charset
     * @throws RuntimeException If the given value cannot be represented in the given charset
     */
    public static void validateCharset( ByteString value, Charset charset ) {
        if ( charset == StandardCharsets.UTF_8 ) {
            final byte[] bytes = value.getBytes();
            if ( !Utf8.isWellFormed( bytes ) ) {
                //CHECKSTYLE: IGNORE 1
                final String string = new String( bytes, charset );
                throw RESOURCE.charsetEncoding( string, charset.name() ).ex();
            }
        }
    }


    /**
     * Makes a name distinct from other names which have already been used, adds it to the list, and returns it.
     *
     * @param name Suggested name, may not be unique
     * @param usedNames Collection of names already used
     * @param suggester Base for name when input name is null
     * @return Unique name
     */
    public static String uniquify( String name, Set<String> usedNames, Suggester suggester ) {
        if ( name != null ) {
            if ( usedNames.add( name ) ) {
                return name;
            }
        }
        final String originalName = name;
        for ( int j = 0; ; j++ ) {
            name = suggester.apply( originalName, j, usedNames.size() );
            if ( usedNames.add( name ) ) {
                return name;
            }
        }
    }


    /**
     * Makes sure that the names in a list are unique.
     *
     * Does not modify the input list. Returns the input list if the strings are unique, otherwise allocates a new list.
     *
     * @param nameList List of strings
     * @param caseSensitive Whether upper and lower case names are considered distinct
     * @return List of unique strings
     */
    public static List<String> uniquify( List<String> nameList, boolean caseSensitive ) {
        return uniquify( nameList, EXPR_SUGGESTER, caseSensitive );
    }


    /**
     * Makes sure that the names in a list are unique.
     *
     * Does not modify the input list. Returns the input list if the strings are unique, otherwise allocates a new list.
     *
     * @param nameList List of strings
     * @param suggester How to generate new names if duplicate names are found
     * @param caseSensitive Whether upper and lower case names are considered distinct
     * @return List of unique strings
     */
    public static List<String> uniquify( List<String> nameList, Suggester suggester, boolean caseSensitive ) {
        final Set<String> used = caseSensitive
                ? new LinkedHashSet<>()
                : new TreeSet<>( String.CASE_INSENSITIVE_ORDER );
        int changeCount = 0;
        final List<String> newNameList = new ArrayList<>();
        for ( String name : nameList ) {
            String uniqueName = uniquify( name, used, suggester );
            if ( !uniqueName.equals( name ) ) {
                ++changeCount;
            }
            newNameList.add( uniqueName );
        }
        return changeCount == 0
                ? nameList
                : newNameList;
    }


    /**
     * Suggests candidates for unique names, given the number of attempts so far and the number of expressions in the project list.
     */
    public interface Suggester {

        String apply( String original, int attempt, int size );

    }


    /**
     * The components of a collation definition, per the SQL standard.
     */
    public static class ParsedCollation {

        private final Charset charset;
        private final Locale locale;
        private final String strength;


        public ParsedCollation( Charset charset, Locale locale, String strength ) {
            this.charset = charset;
            this.locale = locale;
            this.strength = strength;
        }


        public Charset getCharset() {
            return charset;
        }


        public Locale getLocale() {
            return locale;
        }


        public String getStrength() {
            return strength;
        }
    }

}

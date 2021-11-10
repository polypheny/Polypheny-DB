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
import java.util.Locale;
import java.util.StringTokenizer;
import org.apache.calcite.avatica.util.ByteString;
import org.polypheny.db.util.ConversionUtil;
import org.polypheny.db.util.SaffronProperties;

public class CoreUtil {

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

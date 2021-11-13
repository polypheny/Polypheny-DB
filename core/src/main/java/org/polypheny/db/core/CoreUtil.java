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
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import org.apache.calcite.avatica.util.ByteString;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.util.ConversionUtil;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.SaffronProperties;
import org.polypheny.db.util.Util;

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


    public static String deriveAliasFromOrdinal( int ordinal ) {
        // Use a '$' so that queries can't easily reference the generated name.
        return "EXPR$" + ordinal;
    }


    /**
     * Constructs an operator signature from a type list.
     *
     * @param op operator
     * @param typeList list of types to use for operands. Types may be represented as {@link String}, {@link PolyTypeFamily}, or any object with a valid {@link Object#toString()} method.
     * @return constructed signature
     */
    public static String getOperatorSignature( Operator op, List<?> typeList ) {
        return getAliasedSignature( op, op.getName(), typeList );
    }


    /**
     * Constructs an operator signature from a type list, substituting an alias for the operator name.
     *
     * @param op operator
     * @param opName name to use for operator
     * @param typeList list of {@link PolyType} or {@link String} to use for operands
     * @return constructed signature
     */
    public static String getAliasedSignature( Operator op, String opName, List<?> typeList ) {
        StringBuilder ret = new StringBuilder();
        String template = op.getSignatureTemplate( typeList.size() );
        if ( null == template ) {
            ret.append( "'" );
            ret.append( opName );
            ret.append( "(" );
            for ( int i = 0; i < typeList.size(); i++ ) {
                if ( i > 0 ) {
                    ret.append( ", " );
                }
                final String t = typeList.get( i ).toString().toUpperCase( Locale.ROOT );
                ret.append( "<" ).append( t ).append( ">" );
            }
            ret.append( ")'" );
        } else {
            Object[] values = new Object[typeList.size() + 1];
            values[0] = opName;
            ret.append( "'" );
            for ( int i = 0; i < typeList.size(); i++ ) {
                final String t = typeList.get( i ).toString().toUpperCase( Locale.ROOT );
                values[i + 1] = "<" + t + ">";
            }
            ret.append( new MessageFormat( template, Locale.ROOT ).format( values ) );
            ret.append( "'" );
            assert (typeList.size() + 1) == values.length;
        }

        return ret.toString();
    }


    /**
     * Returns whether a node represents the NULL value.
     *
     * Examples:
     *
     * <ul>
     * <li>For {@link Literal} Unknown, returns false.</li>
     * <li>For <code>CAST(NULL AS <i>type</i>)</code>, returns true if <code>allowCast</code> is true, false otherwise.</li>
     * <li>For <code>CAST(CAST(NULL AS <i>type</i>) AS <i>type</i>))</code>, returns false.</li>
     * </ul>
     */
    public static boolean isNullLiteral( Node node, boolean allowCast ) {
        if ( node instanceof Literal ) {
            Literal literal = (Literal) node;
            if ( literal.getTypeName() == PolyType.NULL ) {
                assert null == literal.getValue();
                return true;
            } else {
                // We don't regard UNKNOWN -- SqlLiteral(null,Boolean) -- as NULL.
                return false;
            }
        }
        if ( allowCast ) {
            if ( node.getKind() == Kind.CAST ) {
                Call call = (Call) node;
                if ( isNullLiteral( call.operand( 0 ), false ) ) {
                    // node is "CAST(NULL as type)"
                    return true;
                }
            }
        }
        return false;
    }


    /**
     * Creates the type of an {@link org.polypheny.db.util.NlsString}.
     *
     * The type inherits the NlsString's {@link Charset} and {@link Collation}, if they are set, otherwise it gets the system defaults.
     *
     * @param typeFactory Type factory
     * @param str String
     * @return Type, including collation and charset
     */
    public static RelDataType createNlsStringType( RelDataTypeFactory typeFactory, NlsString str ) {
        Charset charset = str.getCharset();
        if ( null == charset ) {
            charset = typeFactory.getDefaultCharset();
        }
        Collation collation = str.getCollation();
        if ( null == collation ) {
            collation = Collation.COERCIBLE;
        }
        RelDataType type = typeFactory.createPolyType( PolyType.CHAR, str.getValue().length() );
        type = typeFactory.createTypeWithCharsetAndCollation( type, charset, collation );
        return type;
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

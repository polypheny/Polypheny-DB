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

package org.polypheny.db.util;

import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.base.Charsets;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.Casing;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeList;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.nodes.Select;
import org.polypheny.db.runtime.PolyphenyDbContextException;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.runtime.Resources;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;

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
        return switch ( name ) {
            case "BIG5" -> "Big5";
            case "LATIN1" -> "ISO-8859-1";
            case "GB2312", "GBK" -> name;
            case "UTF8" -> "UTF-8";
            case "UTF16" -> Charsets.UTF_16.name();
            case "UTF-16BE", "UTF-16LE", "ISO-8859-1", "UTF-8" -> name;
            default -> null;
        };
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
    public static AlgDataType createNlsStringType( AlgDataTypeFactory typeFactory, NlsString str ) {
        Charset charset = str.getCharset();
        if ( null == charset ) {
            charset = typeFactory.getDefaultCharset();
        }
        Collation collation = str.getCollation();
        if ( null == collation ) {
            collation = Collation.COERCIBLE;
        }
        AlgDataType type = typeFactory.createPolyType( PolyType.CHAR, str.getValue().length() );
        type = typeFactory.createTypeWithCharsetAndCollation( type, charset, collation );
        return type;
    }


    /**
     * Wraps an exception with context.
     */
    public static PolyphenyDbException newContextException( final ParserPos pos, Resources.ExInst<?> e, String inputText ) {
        PolyphenyDbContextException ex = newContextException( pos, e );
        ex.setOriginalStatement( inputText );
        return ex;
    }


    /**
     * Wraps an exception with context.
     */
    public static PolyphenyDbContextException newContextException( final ParserPos pos, Resources.ExInst<?> e ) {
        int line = pos.getLineNum();
        int col = pos.getColumnNum();
        int endLine = pos.getEndLineNum();
        int endCol = pos.getEndColumnNum();
        return newContextException( line, col, endLine, endCol, e );
    }


    /**
     * Wraps an exception with context.
     */
    public static PolyphenyDbContextException newContextException( int line, int col, int endLine, int endCol, Resources.ExInst<?> e ) {
        PolyphenyDbContextException contextExcn =
                (line == endLine && col == endCol
                        ? RESOURCE.validatorContextPoint( line, col )
                        : RESOURCE.validatorContext( line, col, endLine, endCol )).ex( e.ex() );
        contextExcn.setPosition( line, col, endLine, endCol );
        return contextExcn;
    }


    /**
     * Returns the <code>i</code>th select-list item of a query.
     */
    public static Node getSelectListItem( Node query, int i ) {
        switch ( query.getKind() ) {
            case SELECT:
                Select select = (Select) query;
                final Node from = stripAs( select.getFrom() );
                if ( from.getKind() == Kind.VALUES ) {
                    // They wrote "VALUES (x, y)", but the validator has converted this into "SELECT * FROM VALUES (x, y)".
                    return getSelectListItem( from, i );
                }
                final NodeList fields = select.getSelectList();

                // Range check the index to avoid index out of range.  This could be expanded to actually check to see if the select list is a "*"
                if ( i >= fields.size() ) {
                    i = 0;
                }
                return fields.get( i );

            case VALUES:
                Call call = (Call) query;
                assert call.getOperandList().size() > 0 : "VALUES must have at least one operand";
                final Call row = call.operand( 0 );
                assert row.getOperandList().size() > i : "VALUES has too few columns";
                return row.operand( i );

            default:
                // Unexpected type of query.
                throw Util.needToImplement( query );
        }
    }


    /**
     * If a node is "AS", returns the underlying expression; otherwise returns the node.
     */
    public static Node stripAs( Node node ) {
        if ( node != null && node.getKind() == Kind.AS ) {
            return ((Call) node).operand( 0 );
        }
        return node;
    }


    /**
     * @return the character-set prefix of an SQL string literal; returns null if there is none
     */
    public static String getCharacterSet( String s ) {
        if ( s.charAt( 0 ) == '\'' ) {
            return null;
        }
        if ( Character.toUpperCase( s.charAt( 0 ) ) == 'N' ) {
            return SaffronProperties.INSTANCE.defaultNationalCharset().get();
        }
        int i = s.indexOf( "'" );
        return s.substring( 1, i ); // skip prefixed '_'
    }


    /**
     * Converts the contents of an SQL quoted string literal into the corresponding Java string representation
     * (removing leading and trailing quotes and unescaping internal doubled quotes).
     */
    public static String parseString( String s ) {
        int i = s.indexOf( "'" ); // start of body
        if ( i > 0 ) {
            s = s.substring( i );
        }
        return strip( s, "'", "'", "''", Casing.UNCHANGED );
    }


    public static BigDecimal parseDecimal( String s ) {
        return new BigDecimal( s );
    }


    public static BigDecimal parseInteger( String s ) {
        return new BigDecimal( s );
    }


    /**
     * Unquotes a quoted string, using different quotes for beginning and end.
     */
    public static String strip( String s, String startQuote, String endQuote, String escape, Casing casing ) {
        if ( startQuote != null ) {
            assert endQuote != null;
            assert startQuote.length() == 1;
            assert endQuote.length() == 1;
            assert escape != null;
            assert s.startsWith( startQuote ) && s.endsWith( endQuote ) : s;
            s = s.substring( 1, s.length() - 1 ).replace( escape, endQuote );
        }
        switch ( casing ) {
            case TO_UPPER:
                return s.toUpperCase( Locale.ROOT );
            case TO_LOWER:
                return s.toLowerCase( Locale.ROOT );
            default:
                return s;
        }
    }


    /**
     * Trims a string for given characters from left and right. E.g. {@code trim("aBaac123AabC","abBcC")} returns {@code "123A"}.
     */
    public static String trim( String s, String chars ) {
        if ( s.length() == 0 ) {
            return "";
        }

        int start;
        for ( start = 0; start < s.length(); start++ ) {
            char c = s.charAt( start );
            if ( chars.indexOf( c ) < 0 ) {
                break;
            }
        }

        int stop;
        for ( stop = s.length(); stop > start; stop-- ) {
            char c = s.charAt( stop - 1 );
            if ( chars.indexOf( c ) < 0 ) {
                break;
            }
        }

        if ( start >= stop ) {
            return "";
        }

        return s.substring( start, stop );
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


    static public List<Node> toNodeList( List<? extends Node> nodes ) {
        return toNodeList( nodes, Node.class );
    }


    static public <T extends Node> List<T> toNodeList( List<? extends Node> nodes, Class<T> clazz ) {
        return nodes.stream().map( clazz::cast ).collect( Collectors.toList() );
    }

}

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

package org.polypheny.db.sql.language;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.type.DynamicRecordType;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeVisitor;
import org.polypheny.db.sql.language.validate.SqlQualified;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Util;


/**
 * A <code>SqlIdentifier</code> is an identifier, possibly compound.
 */
public class SqlIdentifier extends SqlNode implements Identifier {

    /**
     * Array of the components of this compound identifier.
     * <p>
     * The empty string represents the wildcard "*", to distinguish it from a real "*" (presumably specified using quotes).
     * <p>
     * It's convenient to have this member public, and it's convenient to have this member not-final, but it's a shame it's public and not-final.
     * If you assign to this member, please use {@link #setNames(java.util.List, java.util.List)}.
     * And yes, we'd like to make identifiers immutable one day.
     */
    @Getter
    public ImmutableList<String> names;

    /**
     * This identifier's collation (if any).
     */
    @Getter
    final SqlCollation collation;

    /**
     * A list of the positions of the components of compound identifiers.
     */
    protected ImmutableList<ParserPos> componentPositions;


    /**
     * Creates a compound identifier, for example <code>foo.bar</code>.
     *
     * @param names Parts of the identifier, length &ge; 1
     */
    public SqlIdentifier( List<String> names, SqlCollation collation, ParserPos pos, List<ParserPos> componentPositions ) {
        super( pos );
        this.names = ImmutableList.copyOf( names );
        this.collation = collation;
        this.componentPositions =
                componentPositions == null
                        ? null
                        : ImmutableList.copyOf( componentPositions );
        for ( String name : names ) {
            assert name != null;
        }
    }


    public SqlIdentifier( List<String> names, ParserPos pos ) {
        this( names, null, pos, null );
    }


    /**
     * Creates a simple identifier, for example <code>foo</code>, with a collation.
     */
    public SqlIdentifier( String name, SqlCollation collation, ParserPos pos ) {
        this( ImmutableList.of( name ), collation, pos, null );
    }


    /**
     * Creates a simple identifier, for example <code>foo</code>.
     */
    public SqlIdentifier( String name, ParserPos pos ) {
        this( ImmutableList.of( name ), null, pos, null );
    }


    /**
     * Creates an identifier that is a singleton wildcard star.
     */
    public static SqlIdentifier star( ParserPos pos ) {
        return star( ImmutableList.of( "" ), pos, ImmutableList.of( pos ) );
    }


    /**
     * Creates an identifier that ends in a wildcard star.
     */
    public static SqlIdentifier star( List<String> names, ParserPos pos, List<ParserPos> componentPositions ) {
        return new SqlIdentifier(
                Lists.transform( names, s -> s.equals( "*" ) ? "" : s ),
                null,
                pos,
                componentPositions );
    }


    @Override
    public Kind getKind() {
        return Kind.IDENTIFIER;
    }


    @Override
    public SqlNode clone( ParserPos pos ) {
        return new SqlIdentifier( names, collation, pos, componentPositions );
    }


    @Override
    public String toString() {
        return getString( names );
    }


    /**
     * Converts a list of strings to a qualified identifier.
     */
    public static String getString( List<String> names ) {
        return Util.sepList( toStar( names ), "." );
    }


    /**
     * Converts empty strings in a list of names to stars.
     */
    public static List<String> toStar( List<String> names ) {
        return Lists.transform(
                names,
                s -> s.isEmpty()
                        ? "*"
                        : s.equals( "*" )
                                ? "\"*\""
                                : s );
    }


    /**
     * Modifies the components of this identifier and their positions.
     *
     * @param names Names of components
     * @param poses Positions of components
     */
    public void setNames( List<String> names, List<ParserPos> poses ) {
        this.names = ImmutableList.copyOf( names );
        this.componentPositions =
                poses == null
                        ? null
                        : ImmutableList.copyOf( poses );
    }


    /**
     * Returns an identifier that is the same as this except one modified name.
     * Does not modify this identifier.
     */
    public SqlIdentifier setName( int i, String name ) {
        if ( !names.get( i ).equals( name ) ) {
            String[] nameArray = names.toArray( new String[0] );
            nameArray[i] = name;
            return new SqlIdentifier( ImmutableList.copyOf( nameArray ), collation, pos, componentPositions );
        } else {
            return this;
        }
    }


    /**
     * Returns an identifier that is the same as this except with a component added at a given position. Does not modify this identifier.
     */
    public SqlIdentifier add( int i, String name, ParserPos pos ) {
        final List<String> names2 = new ArrayList<>( names );
        names2.add( i, name );
        final List<ParserPos> pos2;
        if ( componentPositions == null ) {
            pos2 = null;
        } else {
            pos2 = new ArrayList<>( componentPositions );
            pos2.add( i, pos );
        }
        return new SqlIdentifier( names2, collation, pos, pos2 );
    }


    /**
     * Returns the position of the <code>i</code>th component of a compound identifier, or the position of the whole identifier if that information is not present.
     *
     * @param i Ordinal of component.
     * @return Position of i'th component
     */
    public ParserPos getComponentParserPosition( int i ) {
        assert (i >= 0) && (i < names.size());
        return (componentPositions == null)
                ? getPos()
                : componentPositions.get( i );
    }


    /**
     * Copies names and components from another identifier. Does not modify the cross-component parser position.
     *
     * @param other identifier from which to copy
     */
    public void assignNamesFrom( SqlIdentifier other ) {
        setNames( other.names, other.componentPositions );
    }


    /**
     * Creates an identifier which contains only the <code>ordinal</code>th component of this compound identifier. It will have the correct {@link ParserPos}, provided that detailed position information is available.
     */
    public SqlIdentifier getComponent( int ordinal ) {
        return getComponent( ordinal, ordinal + 1 );
    }


    public SqlIdentifier getComponent( int from, int to ) {
        final ParserPos pos;
        final ImmutableList<ParserPos> pos2;
        if ( componentPositions == null ) {
            pos2 = null;
            pos = this.pos;
        } else {
            pos2 = componentPositions.subList( from, to );
            pos = ParserPos.sum( pos2 );
        }
        return new SqlIdentifier( names.subList( from, to ), collation, pos, pos2 );
    }


    /**
     * Creates an identifier that consists of this identifier plus a name segment.
     * Does not modify this identifier.
     */
    public SqlIdentifier plus( String name, ParserPos pos ) {
        final ImmutableList<String> names = ImmutableList.<String>builder().addAll( this.names ).add( name ).build();
        final ImmutableList<ParserPos> componentPositions;
        final ParserPos pos2;
        if ( this.componentPositions != null ) {
            final ImmutableList.Builder<ParserPos> builder = ImmutableList.builder();
            componentPositions = builder.addAll( this.componentPositions ).add( pos ).build();
            pos2 = ParserPos.sum( builder.add( this.pos ).build() );
        } else {
            componentPositions = null;
            pos2 = pos;
        }
        return new SqlIdentifier( names, collation, pos2, componentPositions );
    }


    /**
     * Creates an identifier that consists of this identifier plus a wildcard star.
     * Does not modify this identifier.
     */
    public SqlIdentifier plusStar() {
        final SqlIdentifier id = this.plus( "*", ParserPos.ZERO );
        return new SqlIdentifier(
                id.names.stream().map( s -> s.equals( "*" ) ? "" : s ).collect( Util.toImmutableList() ),
                null,
                id.pos,
                id.componentPositions );
    }


    /**
     * Creates an identifier that consists of all but the last {@code n} name segments of this one.
     */
    public SqlIdentifier skipLast( int n ) {
        return getComponent( 0, names.size() - n );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.IDENTIFIER );
        int i = 0;
        for ( String name : names ) {
            if ( i != 0 || names.size() != 3 || writer.getDialect().supportsColumnNamesWithSchema() ) {
                writer.sep( "." );
                if ( name.isEmpty() ) {
                    writer.print( "*" );
                } else {
                    writer.identifier( name );
                }
            }
            i++;
        }

        if ( null != collation ) {
            collation.unparse( writer, leftPrec, rightPrec );
        }
        writer.endList( frame );
    }


    @Override
    public void validate( SqlValidator validator, SqlValidatorScope scope ) {
        validator.validateIdentifier( this, scope );
    }


    @Override
    public void validateExpr( SqlValidator validator, SqlValidatorScope scope ) {
        // First check for builtin functions which don't have parentheses, like "LOCALTIME".
        SqlCall call = SqlUtil.makeCall( validator.getOperatorTable(), this );
        if ( call != null ) {
            validator.validateCall( call, scope );
            return;
        }

        validator.validateIdentifier( this, scope );
    }


    @Override
    public boolean equalsDeep( Node node, Litmus litmus ) {
        if ( !(node instanceof SqlIdentifier that) ) {
            return litmus.fail( "{} != {}", this, node );
        }
        if ( this.names.size() != that.names.size() ) {
            return litmus.fail( "{} != {}", this, node );
        }
        for ( int i = 0; i < names.size(); i++ ) {
            if ( !this.names.get( i ).equals( that.names.get( i ) ) ) {
                return litmus.fail( "{} != {}", this, node );
            }
        }
        return litmus.succeed();
    }


    @Override
    public @Nullable String getEntity() {
        return String.join( ".", names );
    }


    @Override
    public <R> R accept( NodeVisitor<R> visitor ) {
        return visitor.visit( this );
    }


    @Override
    public String getSimple() {
        assert names.size() == 1;
        return names.get( 0 );
    }


    /**
     * Returns whether this identifier is a star, such as "*" or "foo.bar.*".
     */
    @Override
    public boolean isStar() {
        return Util.last( names ).isEmpty();
    }


    /**
     * Returns whether this is a simple identifier. "FOO" is simple; "*", "FOO.*" and "FOO.BAR" are not.
     */
    @Override
    public boolean isSimple() {
        return names.size() == 1 && !isStar();
    }


    @Override
    public Monotonicity getMonotonicity( SqlValidatorScope scope ) {
        // for "star" column, whether it's static or dynamic return not_monotonic directly.
        if ( Util.last( names ).isEmpty() || DynamicRecordType.isDynamicStarColName( Util.last( names ) ) ) {
            return Monotonicity.NOT_MONOTONIC;
        }

        // First check for builtin functions which don't have parentheses, like "LOCALTIME".
        final SqlValidator validator = scope.getValidator();
        SqlCall call = SqlUtil.makeCall( validator.getOperatorTable(), this );
        if ( call != null ) {
            return call.getMonotonicity( scope );
        }
        final SqlQualified qualified = scope.fullyQualify( this );
        final SqlIdentifier fqId = qualified.identifier;
        return qualified.namespace.resolve().getMonotonicity( Util.last( fqId.names ) );
    }

}


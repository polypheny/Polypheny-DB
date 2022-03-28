/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adapter.neo4j.util;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.enumerable.EnumUtils;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;

public interface NeoStatements {

    enum StatementType {
        MATCH( "MATCH" ),
        CREATE( "CREATE" ),
        WHERE( "WHERE" ),
        RETURN( "RETURN" ),
        WITH( "WITH" );

        public final String identifier;


        StatementType( String identifier ) {
            this.identifier = identifier;
        }
    }


    abstract class NeoStatement {

        public abstract String build();


        public final StatementType type;


        protected NeoStatement( StatementType type ) {
            this.type = type;
        }


        public abstract Expression asExpression();

    }


    abstract class OperatorStatement extends NeoStatement {

        public final ListStatement<?> statements;


        protected OperatorStatement( StatementType type, ListStatement<?> statements ) {
            super( type );
            this.statements = statements;
        }


        @Override
        public String build() {
            return type + " " + statements.build();
        }


        @Override
        public Expression asExpression() {
            return Expressions.new_( getClass(), statements.asExpression() );
        }

    }

    static <T extends NeoStatement> ListStatement<T> list_( List<T> statements ) {
        return new ListStatement<>( statements );
    }

    class ListStatement<T extends NeoStatement> extends NeoStatement {

        private final List<T> statements;


        protected ListStatement( List<T> statements ) {
            super( null );
            this.statements = statements;
        }


        public int size() {
            return statements.size();
        }


        public boolean isEmpty() {
            return statements.isEmpty();
        }


        @Override
        public String build() {
            return statements.stream().map( NeoStatement::build ).collect( Collectors.joining( ", " ) );
        }


        @Override
        public Expression asExpression() {
            return Expressions.new_(
                    getClass(),
                    EnumUtils.expressionList(
                            statements.stream().map( NeoStatement::asExpression ).collect( Collectors.toList() ) ) );
        }

    }


    class NodeStatement extends NeoStatement {

        private final String identifier;
        private final LabelsStatement labels;
        private final ListStatement<?> properties;


        protected NodeStatement( String identifier, LabelsStatement labels, ListStatement<?> properties ) {
            super( null );
            this.identifier = identifier;
            this.labels = labels;
            this.properties = properties;
        }


        @Override
        public String build() {
            return String.format( "( %s%s %s )", identifier, labels.build(), properties.isEmpty() ? "" : String.format( "{ %s }", properties.build() ) );
        }


        @Override
        public Expression asExpression() {
            return Expressions.new_(
                    getClass(),
                    Expressions.constant( identifier, String.class ),
                    labels.asExpression(),
                    properties.asExpression() );
        }

    }

    static NodeStatement node_( String identifier, LabelsStatement labels, PropertyStatement... properties ) {
        return new NodeStatement( identifier, labels, list_( Arrays.asList( properties ) ) );
    }

    static NodeStatement node_( String identifier, LabelsStatement labels, List<PropertyStatement> properties ) {
        return new NodeStatement( identifier, labels, list_( properties ) );
    }

    class PropertyStatement extends NeoStatement {

        private final String key;
        private final NeoStatement value;


        protected PropertyStatement( String key, NeoStatement value ) {
            super( null );
            this.key = key;
            this.value = value;
        }


        @Override
        public String build() {
            return key + ":" + value.build();
        }


        @Override
        public Expression asExpression() {
            return Expressions.new_( getClass(), Expressions.constant( key ), value.asExpression() );
        }

    }

    static PropertyStatement property_( String key, NeoStatement value ) {
        return new PropertyStatement( key, value );
    }


    class LabelsStatement extends NeoStatement {

        private final List<String> labels;


        protected LabelsStatement( List<String> labels ) {
            super( null );
            this.labels = labels;
        }


        @Override
        public String build() {
            return labels.stream().map( l -> ":" + l ).collect( Collectors.joining() );
        }


        @Override
        public Expression asExpression() {
            return Expressions.new_( getClass(), EnumUtils.constantArrayList( labels, String.class ) );
        }

    }

    static LabelsStatement labels_( String... label ) {
        return new LabelsStatement( Arrays.asList( label ) );
    }

    class LiteralStatement extends NeoStatement {

        private final String value;


        protected LiteralStatement( String value ) {
            super( null );
            this.value = value;
        }


        @Override
        public String build() {
            return value;
        }


        @Override
        public Expression asExpression() {
            return Expressions.constant( value, String.class );
        }

    }

    static LiteralStatement literal_( Object value ) {
        return new LiteralStatement( value.toString() );
    }

    static LiteralStatement literal_( RexLiteral literal ) {
        return literal_( NeoUtil.rexAsString( literal ) );
    }

    class AsStatement extends NeoStatement {

        private final NeoStatement key;
        private final NeoStatement alias;


        protected AsStatement( NeoStatement key, NeoStatement alias ) {
            super( null );
            this.key = key;
            this.alias = alias;
        }


        @Override
        public String build() {
            return key.build() + " AS " + alias.build();
        }


        @Override
        public Expression asExpression() {
            return Expressions.new_( getClass(), key.asExpression(), alias.asExpression() );
        }

    }

    static AsStatement as_( NeoStatement key, NeoStatement value ) {
        return new AsStatement( key, value );
    }


    class PreparedStatement extends NeoStatement {

        private final long index;


        protected PreparedStatement( long index ) {
            super( null );
            this.index = index;
        }


        @Override
        public String build() {
            return NeoUtil.asParameter( index, true );
        }


        @Override
        public Expression asExpression() {
            return null;
            //return Expressions.new_( getClass(), Expressions.constant( index ), Expressions.constant( valueType ), Expressions.constant( componentType ) );
        }


    }

    static PreparedStatement prepared_( long index ) {
        return new PreparedStatement( index );
    }

    static PreparedStatement prepared_( RexDynamicParam param ) {
        return new PreparedStatement( param.getIndex() );
    }


    class CreateStatement extends OperatorStatement {

        protected CreateStatement( ListStatement<?> statements ) {
            super( StatementType.CREATE, statements );
        }

    }

    static CreateStatement create_( NeoStatement... statement ) {
        return new CreateStatement( list_( Arrays.asList( statement ) ) );
    }

    static CreateStatement create_( List<NeoStatement> statement ) {
        return new CreateStatement( list_( statement ) );
    }

    class ReturnStatement extends OperatorStatement {

        protected ReturnStatement( ListStatement<?> statements ) {
            super( StatementType.RETURN, statements );
        }

    }

    static ReturnStatement return_( NeoStatement... statement ) {
        return new ReturnStatement( list_( Arrays.asList( statement ) ) );
    }


    class WhereStatement extends OperatorStatement {

        protected WhereStatement( ListStatement<?> statements ) {
            super( StatementType.WHERE, statements );
        }

    }

    static WhereStatement where_( NeoStatement... statement ) {
        return new WhereStatement( list_( Arrays.asList( statement ) ) );
    }


    class MatchStatement extends OperatorStatement {

        protected MatchStatement( ListStatement<?> statements ) {
            super( StatementType.MATCH, statements );
        }

    }

    static MatchStatement match_( NeoStatement... statement ) {
        return new MatchStatement( list_( Arrays.asList( statement ) ) );
    }


    class WithStatement extends OperatorStatement {

        protected WithStatement( ListStatement<?> statements ) {
            super( StatementType.WITH, statements );
        }

    }

    static WithStatement with_( NeoStatement... statement ) {
        return new WithStatement( list_( Arrays.asList( statement ) ) );
    }

}

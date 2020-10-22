/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.adapter.file;


import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.file.FileRel.FileImplementor;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;


public class Update {

    @Getter
    @Setter
    private Integer columnReference;
    private Object literal;
    private Long literalIndex;


    /**
     * Update constructor
     *
     * @param columnReference May be null. Used by generated code, see {@link FileMethod#EXECUTE} and {@link org.polypheny.db.adapter.file.rel.FileToEnumerableConverter#implement}
     * @param literalIndex LiteralIndex for the dataContext
     */
    public Update( final Integer columnReference, final Long literalIndex ) {
        this.columnReference = columnReference;
        this.literalIndex = literalIndex;
    }


    /**
     * Update constructor
     *
     * @param columnReference May be null. Used by generated code, see {@link FileMethod#EXECUTE} and {@link org.polypheny.db.adapter.file.rel.FileToEnumerableConverter#implement}
     * @param literal Literal that will be written in an update
     */
    public Update( final Integer columnReference, final Object literal ) {
        this.columnReference = columnReference;
        this.literal = literal;
    }


    public Object getValue( final DataContext context ) {
        //don't switch these two condition, because a literal assignment can be "null"
        if ( literalIndex != null ) {
            return context.getParameterValue( literalIndex );
        } else {
            return literal;
        }
    }


    Expression getExpression() {
        if ( literal != null ) {
            return Expressions.new_( Update.class, Expressions.constant( columnReference ), Expressions.constant( literal, Object.class ) );
        } else {
            return Expressions.new_( Update.class, Expressions.constant( columnReference ), Expressions.constant( literalIndex, Long.class ) );
        }
    }


    public static Expression getUpdatesExpression( final List<Update> updates ) {
        List<Expression> updateConstructors = new ArrayList<>();
        for ( Update update : updates ) {
            updateConstructors.add( update.getExpression() );
        }
        return Expressions.newArrayInit( Update[].class, updateConstructors );
    }


    public static List<Update> getUpdates( final List<RexNode> exps, FileImplementor implementor ) {
        List<Update> updateList = new ArrayList<>();
        //int offset = exps.size() / 2;
        int offset = implementor.getColumnNames().size();
        for ( int i = 0; i < offset; i++ ) {
            if ( exps.size() > i + offset ) {
                RexNode lit = exps.get( i + offset );
                if ( lit instanceof RexLiteral ) {
                    updateList.add( new Update( null, ((RexLiteral) lit).getValueForQueryParameterizer() ) );
                } else {
                    updateList.add( new Update( null, ((RexDynamicParam) lit).getIndex() ) );
                }
            }
        }
        return updateList;
    }
}

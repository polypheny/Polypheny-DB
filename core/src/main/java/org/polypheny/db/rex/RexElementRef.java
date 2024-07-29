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

package org.polypheny.db.rex;

import lombok.Getter;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.type.entity.PolyValue;

@Getter
public class RexElementRef extends RexVariable {

    private final RexNode collectionRef;


    public RexElementRef( RexNode collectionRef, AlgDataType type ) {
        super( "$elem" + collectionRef, type );
        this.collectionRef = collectionRef;
    }


    @Override
    public <R> R accept( RexVisitor<R> visitor ) {
        return visitor.visitElementRef( this );
    }


    @Override
    public <R, P> R accept( RexBiVisitor<R, P> visitor, P arg ) {
        return visitor.visitElementRef( this, arg );
    }


    @Override
    public Kind getKind() {
        return Kind.ELEMENT_REF;
    }


    public Expression getExpression() {
        return Expressions.parameter( PolyValue.class, "_elem$" );
    }

}

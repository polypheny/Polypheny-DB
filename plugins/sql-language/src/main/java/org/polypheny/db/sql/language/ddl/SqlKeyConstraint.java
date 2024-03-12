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

package org.polypheny.db.sql.language.ddl;


import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.logistic.ConstraintType;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code UNIQUE}, {@code PRIMARY KEY} constraints.
 * <p>
 * And {@code FOREIGN KEY}, when we support it.
 */
@EqualsAndHashCode(callSuper = true)
@Getter
@Value
@NonFinal
public class SqlKeyConstraint extends SqlCall {

    public static final SqlSpecialOperator UNIQUE = new SqlSpecialOperator( "UNIQUE", Kind.UNIQUE );

    public static final SqlSpecialOperator PRIMARY = new SqlSpecialOperator( "PRIMARY KEY", Kind.PRIMARY_KEY );

    public static final SqlSpecialOperator FOREIGN = new SqlSpecialOperator( "FOREIGN KEY", Kind.FOREIGN_KEY );

    SqlIdentifier name;

    SqlNodeList fields;


    /**
     * Creates a SqlKeyConstraint.
     */
    SqlKeyConstraint( ParserPos pos, SqlIdentifier name, SqlNodeList fields ) {
        super( pos );
        this.name = name;
        this.fields = fields;
    }


    /**
     * Creates a UNIQUE constraint.
     */
    public static SqlKeyConstraint unique( ParserPos pos, SqlIdentifier name, SqlNodeList fields ) {
        return new SqlKeyConstraint( pos, name, fields );
    }


    /**
     * Creates a PRIMARY KEY constraint.
     */
    public static SqlKeyConstraint primary( ParserPos pos, SqlIdentifier name, SqlNodeList fields ) {
        return new SqlKeyConstraint( pos, name, fields ) {
            @Override
            public Operator getOperator() {
                return PRIMARY;
            }

        };
    }


    public ConstraintType getConstraintType() {
        if ( getOperator().equals( PRIMARY ) ) {
            return ConstraintType.PRIMARY;
        } else if ( getOperator().equals( FOREIGN ) ) {
            return ConstraintType.FOREIGN;
        } else {
            return ConstraintType.UNIQUE;
        }
    }


    @Override
    public Operator getOperator() {
        return UNIQUE;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( name, fields );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( name, fields );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        if ( name != null ) {
            writer.keyword( "CONSTRAINT" );
            name.unparse( writer, 0, 0 );
        }
        writer.keyword( getOperator().getName() ); // "UNIQUE" or "PRIMARY KEY"
        fields.unparse( writer, 1, 1 );
    }

}


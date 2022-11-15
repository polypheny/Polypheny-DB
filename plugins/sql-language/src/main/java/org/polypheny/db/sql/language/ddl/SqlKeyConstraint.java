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

package org.polypheny.db.sql.language.ddl;


import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog.ConstraintType;
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
 *
 * And {@code FOREIGN KEY}, when we support it.
 */
public class SqlKeyConstraint extends SqlCall {

    public static final SqlSpecialOperator UNIQUE = new SqlSpecialOperator( "UNIQUE", Kind.UNIQUE );

    public static final SqlSpecialOperator PRIMARY = new SqlSpecialOperator( "PRIMARY KEY", Kind.PRIMARY_KEY );

    @Getter
    private final SqlIdentifier name;
    @Getter
    private final SqlNodeList columnList;


    /**
     * Creates a SqlKeyConstraint.
     */
    SqlKeyConstraint( ParserPos pos, SqlIdentifier name, SqlNodeList columnList ) {
        super( pos );
        this.name = name;
        this.columnList = columnList;
    }


    /**
     * Creates a UNIQUE constraint.
     */
    public static SqlKeyConstraint unique( ParserPos pos, SqlIdentifier name, SqlNodeList columnList ) {
        return new SqlKeyConstraint( pos, name, columnList );
    }


    /**
     * Creates a PRIMARY KEY constraint.
     */
    public static SqlKeyConstraint primary( ParserPos pos, SqlIdentifier name, SqlNodeList columnList ) {
        return new SqlKeyConstraint( pos, name, columnList ) {
            @Override
            public Operator getOperator() {
                return PRIMARY;
            }

        };
    }


    public ConstraintType getConstraintType() {
        if ( getOperator().equals( PRIMARY ) ) {
            return ConstraintType.PRIMARY;
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
        return ImmutableNullableList.of( name, columnList );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( name, columnList );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        if ( name != null ) {
            writer.keyword( "CONSTRAINT" );
            name.unparse( writer, 0, 0 );
        }
        writer.keyword( getOperator().getName() ); // "UNIQUE" or "PRIMARY KEY"
        columnList.unparse( writer, 1, 1 );
    }

}


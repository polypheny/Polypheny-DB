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

package org.polypheny.db.document.rules;

import lombok.Getter;
import lombok.experimental.Accessors;
import org.polypheny.db.mql.fun.MqlFunctionOperator;
import org.polypheny.db.rel.SingleRel;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.fun.SqlDocEqualExpressionOperator;
import org.polypheny.db.sql.fun.SqlJsonValueFunction;


/**
 * Helper functions, which can be used to exclude document logics during planing from stores,
 * which are not able to support it
 */
public class DocumentRules {

    public static boolean containsJson( SingleRel rel ) {
        JsonVisitor visitor = new JsonVisitor();
        for ( RexNode node : rel.getChildExps() ) {
            node.accept( visitor );
            if ( visitor.containsJson() ) {
                return true;
            }
        }
        return false;
    }


    public static boolean containsDocument( SingleRel project ) {
        DocumentVisitor visitor = new DocumentVisitor();
        for ( RexNode node : project.getChildExps() ) {
            node.accept( visitor );
            if ( visitor.containsDocument() ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Visitor, which returns false if a JSON RexCall was traversed
     */
    private static class JsonVisitor extends RexVisitorImpl<Void> {

        @Accessors(fluent = true)
        @Getter
        private boolean containsJson = false;


        protected JsonVisitor() {
            super( true );
        }


        @Override
        public Void visitCall( RexCall call ) {
            SqlOperator operator = call.getOperator();
            if ( operator.kind == SqlKind.JSON_VALUE_EXPRESSION
                    || operator instanceof SqlJsonValueFunction
                    || operator.kind == SqlKind.JSON_API_COMMON_SYNTAX
                    || operator instanceof SqlDocEqualExpressionOperator ) {
                containsJson = true;
            }
            return super.visitCall( call );
        }

    }


    /**
     * Visitor, which returns false if any RexCall, which handles the Document model was traversed
     */
    private static class DocumentVisitor extends RexVisitorImpl<Void> {

        @Accessors(fluent = true)
        @Getter
        private boolean containsDocument = false;


        protected DocumentVisitor() {
            super( true );
        }


        @Override
        public Void visitCall( RexCall call ) {
            SqlOperator operator = call.getOperator();
            if ( operator instanceof MqlFunctionOperator ) {
                containsDocument = true;
            }
            return super.visitCall( call );
        }

    }

}

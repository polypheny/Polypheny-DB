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

package org.polypheny.db.schema.document;

import java.util.Arrays;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.nodes.Function.FunctionType;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexVisitorImpl;


/**
 * Helper functions, which can be used to exclude document logics during planing from stores, which are not able to support it.
 */
public class DocumentRules {

    public static boolean containsJson( SingleAlg alg ) {
        JsonVisitor visitor = new JsonVisitor();
        for ( RexNode node : alg.getChildExps() ) {
            node.accept( visitor );
            if ( visitor.containsJson() ) {
                return true;
            }
        }
        return false;
    }


    public static boolean containsDocument( SingleAlg project ) {
        DocumentVisitor visitor = new DocumentVisitor();
        for ( RexNode node : project.getChildExps() ) {
            node.accept( visitor );
            if ( visitor.containsDocument() ) {
                return true;
            }
        }
        return false;
    }


    @Getter
    public static class DocUpdateVisitor extends RexVisitorImpl<Void> {

        boolean containsUpdate = false;


        protected DocUpdateVisitor() {
            super( true );
        }


        @Override
        public Void visitCall( RexCall call ) {
            if ( Arrays.asList( Kind.MQL_UPDATE, Kind.MQL_UPDATE_REMOVE, Kind.MQL_UPDATE_RENAME, Kind.MQL_UPDATE_REPLACE ).contains( call.op.getKind() ) ) {
                containsUpdate = true;
            }

            return super.visitCall( call );
        }

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
            Operator operator = call.getOperator();
            if ( operator.getKind() == Kind.JSON_VALUE_EXPRESSION
                    || operator.getFunctionType() == FunctionType.JSON_VALUE
                    || operator.getKind() == Kind.JSON_API_COMMON_SYNTAX ) {
                containsJson = true;
            }
            return super.visitCall( call );
        }

    }


    /**
     * Visitor, which returns false if any RexCall, which handles the Document model was traversed
     */
    @Getter
    private static class DocumentVisitor extends RexVisitorImpl<Void> {

        @Accessors(fluent = true)
        private boolean containsDocument = false;


        protected DocumentVisitor() {
            super( true );
        }


        @Override
        public Void visitCall( RexCall call ) {
            Operator operator = call.getOperator();
            if ( Kind.MQL_KIND.contains( operator.getKind() ) ) {
                containsDocument = true;
            }
            return super.visitCall( call );
        }

    }

}

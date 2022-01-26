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

package org.polypheny.db.cypher;

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.cypher.CypherGate.Gate;
import org.polypheny.db.cypher.admin.CypherWithGraph;
import org.polypheny.db.cypher.clause.CypherCall;
import org.polypheny.db.cypher.clause.CypherCase;
import org.polypheny.db.cypher.clause.CypherClause;
import org.polypheny.db.cypher.clause.CypherCreate;
import org.polypheny.db.cypher.clause.CypherCreateConstraint;
import org.polypheny.db.cypher.clause.CypherDeleteClause;
import org.polypheny.db.cypher.clause.CypherDropConstraint;
import org.polypheny.db.cypher.clause.CypherForeach;
import org.polypheny.db.cypher.clause.CypherLoadCSV;
import org.polypheny.db.cypher.clause.CypherMatchClause;
import org.polypheny.db.cypher.clause.CypherMergeClause;
import org.polypheny.db.cypher.clause.CypherOrderItem;
import org.polypheny.db.cypher.clause.CypherQuery;
import org.polypheny.db.cypher.clause.CypherRelPattern;
import org.polypheny.db.cypher.clause.CypherRemoveClause;
import org.polypheny.db.cypher.clause.CypherReturn;
import org.polypheny.db.cypher.clause.CypherReturnClause;
import org.polypheny.db.cypher.clause.CypherReturnItem;
import org.polypheny.db.cypher.clause.CypherReturns;
import org.polypheny.db.cypher.clause.CypherSetClause;
import org.polypheny.db.cypher.clause.CypherShowConstraint;
import org.polypheny.db.cypher.clause.CypherShowFunction;
import org.polypheny.db.cypher.clause.CypherShowIndex;
import org.polypheny.db.cypher.clause.CypherShowProcedure;
import org.polypheny.db.cypher.clause.CypherShowTransactions;
import org.polypheny.db.cypher.clause.CypherTerminateTransaction;
import org.polypheny.db.cypher.clause.CypherUseClause;
import org.polypheny.db.cypher.clause.CypherWhere;
import org.polypheny.db.cypher.clause.CypherWith;
import org.polypheny.db.cypher.ddl.CypherSchemaCommand;
import org.polypheny.db.cypher.expression.CypherComparison;
import org.polypheny.db.cypher.expression.CypherExistSubQuery;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.cypher.expression.CypherExpression.Expression;
import org.polypheny.db.cypher.expression.CypherFormula;
import org.polypheny.db.cypher.expression.CypherFunctionInvocation;
import org.polypheny.db.cypher.expression.CypherListComprehension;
import org.polypheny.db.cypher.expression.CypherListLookup;
import org.polypheny.db.cypher.expression.CypherListSlice;
import org.polypheny.db.cypher.expression.CypherLiteral;
import org.polypheny.db.cypher.expression.CypherLiteral.Literal;
import org.polypheny.db.cypher.expression.CypherMapProjection;
import org.polypheny.db.cypher.expression.CypherPatternComprehension;
import org.polypheny.db.cypher.expression.CypherProperty;
import org.polypheny.db.cypher.expression.CypherReduceExpression;
import org.polypheny.db.cypher.expression.CypherUnary;
import org.polypheny.db.cypher.expression.CypherUseGraph;
import org.polypheny.db.cypher.expression.CypherVariable;
import org.polypheny.db.cypher.hint.CypherHint;
import org.polypheny.db.cypher.hint.CypherIndexHint;
import org.polypheny.db.cypher.mapprojection.CypherMPAll;
import org.polypheny.db.cypher.mapprojection.CypherMPLiteral;
import org.polypheny.db.cypher.mapprojection.CypherMPProperty;
import org.polypheny.db.cypher.mapprojection.CypherMPVariable;
import org.polypheny.db.cypher.parser.StringPos;
import org.polypheny.db.cypher.pattern.CypherEveryPathPattern;
import org.polypheny.db.cypher.pattern.CypherNamedPattern;
import org.polypheny.db.cypher.pattern.CypherNodePattern;
import org.polypheny.db.cypher.pattern.CypherPattern;
import org.polypheny.db.cypher.pattern.CypherShortestPathPattern;
import org.polypheny.db.cypher.query.CypherPeriodicCommit;
import org.polypheny.db.cypher.query.CypherSingleQuery;
import org.polypheny.db.cypher.query.CypherUnion;
import org.polypheny.db.cypher.remove.CypherRemoveItem;
import org.polypheny.db.cypher.remove.CypherRemoveLabels;
import org.polypheny.db.cypher.remove.CypherRemoveProperty;
import org.polypheny.db.cypher.set.CypherSetItem;
import org.polypheny.db.cypher.set.CypherSetLabels;
import org.polypheny.db.cypher.set.CypherSetProperty;
import org.polypheny.db.cypher.set.CypherSetVariable;
import org.polypheny.db.languages.ParserPos;

public interface CypherFactory {

    static ParserPos inputPosition( int beginOffset, int beginLine, int beginColumn ) {
        return new ParserPos( beginLine, beginColumn );
    }

    static CypherQuery createPeriodicCommit( ParserPos pos, ParserPos pos1, String s, CypherClause loadCsv, List<CypherClause> queryBody ) {
        return new CypherPeriodicCommit( pos, pos1, s, loadCsv, queryBody );
    }

    static CypherQuery createUnion( ParserPos pos, CypherQuery lhs, CypherQuery rhs, boolean all ) {
        return new CypherUnion( pos, lhs, rhs, all );
    }

    static CypherQuery createSingleQuery( List<CypherClause> clauses ) {
        return new CypherSingleQuery( clauses );
    }

    static CypherReturnClause createReturn( ParserPos pos, boolean distinct, List<CypherReturn> returnItems, List<CypherOrderItem> order, ParserPos pos1, CypherExpression skip, ParserPos pos2, CypherExpression limit, ParserPos pos3 ) {
        return new CypherReturnClause( pos, distinct, returnItems, order, pos1, skip, pos2, limit, pos3 );
    }

    static CypherReturn createReturnItem( ParserPos pos, CypherExpression e, CypherVariable v ) {
        return new CypherReturnItem( pos, e, v );
    }

    static CypherReturn createReturnItem( ParserPos pos, CypherExpression e, int beginOffset, int endOffset ) {
        return new CypherReturnItem( pos, e, beginOffset, endOffset );
    }

    static CypherReturns createReturnItems( ParserPos pos, boolean returnAll, List<CypherReturn> returnItems ) {
        return new CypherReturns( pos, returnAll, returnItems );
    }

    static CypherOrderItem createOrderItem( ParserPos pos, boolean b, CypherExpression e ) {
        return new CypherOrderItem( pos, b, e );
    }

    static CypherWhere createWhere( ParserPos pos, CypherExpression e ) {
        return new CypherWhere( pos, e );
    }

    static CypherClause createWith( ParserPos pos, CypherReturnClause returnClause, CypherWhere where ) {
        return new CypherWith( pos, returnClause, where );
    }

    static CypherClause createCreate( ParserPos pos, List<CypherPattern> patterns ) {
        return new CypherCreate( pos, patterns );
    }

    static CypherSetClause createSet( ParserPos pos, List<CypherSetItem> items ) {
        return new CypherSetClause( pos, items );
    }

    static CypherSetItem createSetProperty( CypherProperty p, CypherExpression e ) {
        return new CypherSetProperty( p, e );
    }

    static CypherSetItem createSetVariable( CypherVariable v, CypherExpression e, boolean b ) {
        return new CypherSetVariable( v, e, b );
    }

    static CypherSetItem createSetLabels( CypherVariable variable, List<StringPos<ParserPos>> labels ) {
        return new CypherSetLabels( variable, labels );
    }

    static CypherClause createRemove( ParserPos pos, List<CypherRemoveItem> items ) {
        return new CypherRemoveClause( pos, items );
    }

    static CypherRemoveItem createRemoveProperty( CypherProperty property ) {
        return new CypherRemoveProperty( property );
    }

    static CypherRemoveItem createRemoveLabels( CypherVariable variable, List<StringPos<ParserPos>> labels ) {
        return new CypherRemoveLabels( variable, labels );
    }

    static CypherClause createDelete( ParserPos pos, boolean detach, List<CypherExpression> list ) {
        return new CypherDeleteClause( pos, detach, list );
    }

    static CypherClause createMatch( ParserPos pos, boolean optional, List<CypherPattern> patterns, ParserPos pos1, List<CypherHint> hints, CypherWhere where ) {
        return new CypherMatchClause( pos, optional, patterns, pos1, hints, where );
    }

    static CypherHint createIndexHint( ParserPos pos, CypherVariable variable, String labelOrRelType, List<String> propNames, boolean seek, HintIndexType indexType ) {
        return new CypherIndexHint( pos, variable, labelOrRelType, propNames, seek, indexType );
    }

    static CypherClause createMerge( ParserPos pos, CypherPattern pattern, ArrayList<CypherSetClause> clauses, ArrayList<MergeActionType> actionTypes, ArrayList<ParserPos> positions ) {
        return new CypherMergeClause( pos, pattern, clauses, actionTypes, positions );
    }

    static CypherClause createCall( ParserPos pos, ParserPos nextPos, ParserPos procedurePos, ParserPos resultPos, List<String> namespace, String name, List<CypherExpression> arguments, boolean yieldAll, List<CypherCallResultItem> items, CypherWhere where ) {
        return new CypherCall( pos, nextPos, procedurePos, resultPos, namespace, name, arguments, yieldAll, items, where );
    }

    static CypherCallResultItem createCallResultItem( ParserPos pos, String expression, CypherVariable variable ) {
        return new CypherCallResultItem( pos, expression, variable );
    }

    static CypherClause createLoadCSV( ParserPos pos, boolean headers, CypherExpression source, CypherVariable variable, String seperator ) {
        return new CypherLoadCSV( pos, headers, source, variable, seperator );
    }

    static CypherClause createForeach( ParserPos pos, CypherVariable variable, CypherExpression expression, List<CypherClause> clauses ) {
        return new CypherForeach( pos, variable, expression, clauses );
    }

    static CypherInTransactionParams createInTransactionParams( ParserPos pos, CypherExpression batchSize ) {
        return new CypherInTransactionParams( pos, batchSize );
    }

    static CypherPattern createNamedPattern( CypherVariable variable, CypherPattern anonymousPattern ) {
        return new CypherNamedPattern( variable, anonymousPattern );
    }

    static CypherPattern createShortestPathPattern( ParserPos pos, CypherPattern pathPattern, boolean all ) {
        return new CypherShortestPathPattern( pos, pathPattern, all );
    }

    static CypherPattern createEveryPathPattern( List<CypherNodePattern> nodes, List<CypherRelPattern> relationships ) {
        return new CypherEveryPathPattern( nodes, relationships );
    }

    static CypherNodePattern createNodePattern( ParserPos pos, CypherVariable variable, List<StringPos<ParserPos>> labels, CypherExpression properties, CypherExpression predicate ) {
        return new CypherNodePattern( pos, variable, labels, properties );
    }

    static CypherExpression createHasLabelOrTypes( CypherExpression subject, List<StringPos<ParserPos>> labels ) {
        return new CypherHasLabelOrTypes( subject, labels );
    }


    static CypherRelPattern createRelationshipPattern( ParserPos pos, boolean left, boolean right, CypherVariable variable, List<StringPos<ParserPos>> relTypes, CypherPathLength pathLength, CypherExpression properties, CypherExpression predicate, boolean legacyTypeSeparator ) {
        return new CypherRelPattern( pos, left, right, variable, relTypes, pathLength, properties, predicate, legacyTypeSeparator );
    }

    static CypherPathLength createPathLength( ParserPos pos, ParserPos fromPos, ParserPos toPos, String from, String to ) {
        return new CypherPathLength( pos, fromPos, toPos, from, to );
    }

    static CypherExpression createGate( ParserPos pos, Gate gate, CypherExpression left, CypherExpression right ) {
        return new CypherGate( pos, gate, left, right );
    }

    static CypherExpression createGate( Gate gate, List<CypherExpression> expressions ) {
        return new CypherGate( gate, expressions );
    }

    static CypherExpression createBinary( ParserPos pos, OperatorName op, CypherExpression left, CypherExpression right ) {
        return new CypherBinary( pos, op, left, right );
    }

    static CypherExpression startsWith( ParserPos pos, CypherExpression left, CypherExpression right ) {
        return new CypherComparison( pos, OperatorName.STARTS_WITH, left, right );
    }

    static CypherExpression endsWith( ParserPos pos, CypherExpression left, CypherExpression right ) {
        return new CypherComparison( pos, OperatorName.ENDS_WITH, left, right );
    }

    static CypherExpression regeq( ParserPos pos, CypherExpression left, CypherExpression right ) {
        return new CypherComparison( pos, OperatorName.REG_EQUALS, left, right );
    }

    static CypherExpression contains( ParserPos pos, CypherExpression left, CypherExpression right ) {
        return new CypherComparison( pos, OperatorName.CONTAINS, left, right );
    }

    static CypherExpression in( ParserPos pos, CypherExpression left, CypherExpression right ) {
        return new CypherComparison( pos, OperatorName.IN, left, right );
    }

    static CypherExpression isNull( ParserPos pos, CypherExpression left ) {
        return new CypherComparison( pos, OperatorName.IS_NULL, left, null );
    }

    static CypherExpression isNotNull( ParserPos pos, CypherExpression left ) {
        return new CypherComparison( pos, OperatorName.IS_NOT_NULL, left, null );
    }

    static CypherExpression plus( ParserPos pos, CypherExpression left, CypherExpression right ) {
        return new CypherFormula( pos, OperatorName.PLUS, left, right );
    }


    static CypherExpression minus( ParserPos pos, CypherExpression left, CypherExpression right ) {
        return new CypherFormula( pos, OperatorName.MINUS, left, right );
    }

    static CypherExpression multiply( ParserPos pos, CypherExpression left, CypherExpression right ) {
        return new CypherFormula( pos, OperatorName.MULTIPLY, left, right );
    }

    static CypherExpression divide( ParserPos pos, CypherExpression left, CypherExpression right ) {
        return new CypherFormula( pos, OperatorName.DIVIDE, left, right );
    }

    static CypherExpression modulo( ParserPos pos, CypherExpression left, CypherExpression right ) {
        return new CypherFormula( pos, OperatorName.MOD, left, right );
    }

    static CypherExpression pow( ParserPos pos, CypherExpression left, CypherExpression right ) {
        return new CypherFormula( pos, OperatorName.POWER, left, right );
    }

    static CypherExpression unaryPlus( ParserPos pos, CypherExpression expression ) {
        return new CypherUnary( pos, OperatorName.UNARY_PLUS, expression );
    }

    static CypherExpression unaryMinus( ParserPos pos, CypherExpression expression ) {
        return new CypherUnary( pos, OperatorName.UNARY_MINUS, expression );
    }

    static CypherExpression listLookup( CypherExpression subject, CypherExpression list ) {
        return new CypherListLookup( subject, list );
    }

    static CypherExpression listSlice( ParserPos pos, CypherExpression subject, CypherExpression from, CypherExpression to ) {
        return new CypherListSlice( pos, subject, from, to );
    }

    static CypherExpression newTrueLiteral( ParserPos pos ) {
        return new CypherLiteral( pos, Literal.TRUE );
    }

    static CypherExpression newFalseLiteral( ParserPos pos ) {
        return new CypherLiteral( pos, Literal.FALSE );
    }

    static CypherExpression newNullLiteral( ParserPos pos ) {
        return new CypherLiteral( pos, Literal.NULL );
    }

    static CypherExpression newCountStar( ParserPos pos ) {
        return new CypherLiteral( pos, Literal.STAR );
    }

    static CypherExpression caseExpression( ParserPos pos, CypherExpression condition, List<CypherExpression> when, List<CypherExpression> then, CypherExpression elseCase ) {
        return new CypherCase( pos, condition, when, then, elseCase );
    }

    static CypherExpression listComprehension( ParserPos pos, CypherVariable variable, CypherExpression list, CypherExpression where, CypherExpression projection ) {
        return new CypherListComprehension( pos, variable, list, where, projection );
    }

    static CypherExpression patternComprehension( ParserPos pos, ParserPos patternPos, CypherVariable variable, CypherPattern pattern, CypherExpression where, CypherExpression projection ) {
        return new CypherPatternComprehension( pos, patternPos, pattern, variable, where, projection );
    }

    static CypherExpression reduceExpression( ParserPos pos, CypherVariable acc, CypherExpression accExpr, CypherVariable variable, CypherExpression varExpr, CypherExpression innerExpr ) {
        return new CypherReduceExpression( pos, acc, accExpr, variable, varExpr, innerExpr );
    }

    static CypherExpression allExpression( ParserPos pos, CypherVariable variable, CypherExpression expression, CypherExpression where ) {
        return new CypherExpression( pos, Expression.ALL, variable, expression, where );
    }

    static CypherExpression anyExpression( ParserPos pos, CypherVariable variable, CypherExpression expression, CypherExpression where ) {
        return new CypherExpression( pos, Expression.ANY, variable, expression, where );
    }

    static CypherExpression noneExpression( ParserPos pos, CypherVariable variable, CypherExpression expression, CypherExpression where ) {
        return new CypherExpression( pos, Expression.NONE, variable, expression, where );
    }

    static CypherExpression singleExpression( ParserPos pos, CypherVariable variable, CypherExpression expression, CypherExpression where ) {
        return new CypherExpression( pos, Expression.SINGLE, variable, expression, where );
    }

    static CypherExpression patternExpression( ParserPos pos, CypherPattern pattern ) {
        return new CypherExpression( pos, Expression.PATTERN, pattern );
    }

    static CypherExpression mapProjection( ParserPos pos, CypherVariable variable, List<CypherMapProjectionItem> items ) {
        return new CypherMapProjection( pos, variable, items );
    }

    static CypherMapProjectionItem mapProjectionLiteralEntry( StringPos<ParserPos> pos, CypherExpression expression ) {
        return new CypherMPLiteral( pos, expression );
    }

    static CypherMapProjectionItem mapProjectionProperty( StringPos<ParserPos> pos ) {
        return new CypherMPProperty( pos );
    }

    static CypherMapProjectionItem mapProjectionVariable( CypherVariable variable ) {
        return new CypherMPVariable( variable );
    }

    static CypherMapProjectionItem mapProjectionAll( ParserPos pos ) {
        return new CypherMPAll( pos );
    }

    static CypherExpression existsSubQuery( ParserPos pos, List<CypherPattern> patterns, CypherExpression where ) {
        return new CypherExistSubQuery( pos, patterns, where );
    }

    static CypherExpression listLiteral( ParserPos pos, List<CypherExpression> list ) {
        return new CypherLiteral( pos, Literal.LIST, list );
    }

    static CypherExpression mapLiteral( ParserPos pos, List<StringPos<ParserPos>> keys, List<CypherExpression> values ) {
        return new CypherLiteral( pos, Literal.MAP, keys, values );
    }

    static CypherExpression newString( ParserPos pos, String image ) {
        return new CypherLiteral( pos, Literal.STRING, image );
    }

    static CypherExpression newDouble( ParserPos pos, String image ) {
        return new CypherLiteral( pos, Literal.DOUBLE, image );
    }

    static CypherExpression newDecimalInteger( ParserPos pos, String image, boolean negated ) {
        return new CypherLiteral( pos, Literal.DECIMAL, image, negated );
    }

    static CypherExpression newHexInteger( ParserPos pos, String image, boolean negated ) {
        return new CypherLiteral( pos, Literal.HEX, image, negated );
    }

    static CypherExpression newOctalInteger( ParserPos pos, String image, boolean negated ) {
        return new CypherLiteral( pos, Literal.OCTAL, image, negated );
    }

    static CypherExpression functionInvocation( ParserPos beforePos, ParserPos namePos, List<String> namespace, String image, boolean distinct, List<CypherExpression> arguments ) {
        return new CypherFunctionInvocation( beforePos, namePos, namespace, image, distinct, arguments );
    }

    static CypherVariable newVariable( ParserPos pos, String image ) {
        return new CypherVariable( pos, image );
    }

    static CypherWithGraph useGraph( CypherWithGraph statement, CypherUseClause useClause ) {
        return new CypherUseGraph( statement, useClause );
    }

    static CypherReturn newReturnItem( ParserPos pos, CypherVariable expression, int beginOffset, int endOffset ) {
        return new CypherReturnItem( pos, expression, beginOffset, endOffset );
    }

    static CypherReturn newReturnItem( ParserPos pos, CypherVariable expression, CypherVariable variable ) {
        return new CypherReturnItem( pos, expression, variable );
    }

    static CypherYield yieldClause( ParserPos pos, boolean returnAll, List<CypherReturn> returnItems, ParserPos nextPos, List<CypherOrderItem> orders, ParserPos skipPos, CypherExpression skip, ParserPos limitPos, CypherExpression limit, ParserPos wherePos, CypherWhere where ) {
        return new CypherYield( pos, returnAll, returnItems, nextPos, orders, skipPos, skip, limitPos, limit, wherePos, where );
    }

    static CypherClause showIndexClause( ParserPos pos, ShowCommandFilterType indexType, boolean brief, boolean verbose, CypherWhere where, boolean yield ) {
        return new CypherShowIndex( pos, indexType, brief, verbose, where, yield );
    }

    static CypherStatement newSingleQuery( ParserPos pos, List<CypherClause> clauses ) {
        return new CypherSingleQuery( pos, clauses );
    }

    static CypherClause showConstraintClause( ParserPos pos, ShowCommandFilterType constraintType, boolean brief, boolean verbose, CypherWhere where, boolean yield ) {
        return new CypherShowConstraint( pos, constraintType, brief, verbose, where, yield );
    }

    static CypherClause showProcedureClause( ParserPos pos, boolean currentUser, String user, CypherWhere where, boolean yield ) {
        return new CypherShowProcedure( pos, currentUser, user, where, yield );
    }

    static CypherClause showFunctionClause( ParserPos pos, ShowCommandFilterType functionType, boolean currentUser, String user, CypherWhere where, boolean yield ) {
        return new CypherShowFunction( pos, functionType, currentUser, user, where, yield );
    }

    static CypherClause showTransactionsClause( ParserPos pos, CypherSimpleEither<List<String>, CypherParameter> idEither, CypherWhere where, boolean yield ) {
        return new CypherShowTransactions( pos, idEither, where, yield );
    }

    static CypherClause terminateTransactionsClause( ParserPos pos, CypherSimpleEither<List<String>, CypherParameter> idEither ) {
        return new CypherTerminateTransaction( pos, idEither );
    }

    static CypherSchemaCommand createConstraint( ParserPos pos, ConstraintType constraintType, boolean replace, boolean ifNotExists, String name, CypherVariable variable, StringPos<ParserPos> parserPosStringPos, List<CypherProperty> properties, CypherSimpleEither options, boolean containsOn, ConstraintVersion constraintVersion ) {
        return new CypherCreateConstraint( pos, constraintType, replace, ifNotExists, name, variable, parserPosStringPos, properties, options, containsOn, constraintVersion );
    }

    static CypherSchemaCommand dropConstraint( ParserPos pos, ConstraintType constraintType, CypherVariable variable, StringPos<ParserPos> parserPosStringPos, List<CypherProperty> properties ) {
        return new CypherDropConstraint( pos, constraintType, variable, parserPosStringPos, properties );
    }

}

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

package org.polypheny.db.cypher;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.cypher.CypherResource.ResourceType;
import org.polypheny.db.cypher.admin.CypherAdminAction;
import org.polypheny.db.cypher.admin.CypherAdminCommand;
import org.polypheny.db.cypher.admin.CypherAdminCommand.AccessType;
import org.polypheny.db.cypher.admin.CypherAlterNamespace;
import org.polypheny.db.cypher.admin.CypherAlterNamespaceAlias;
import org.polypheny.db.cypher.admin.CypherCreateNamespace;
import org.polypheny.db.cypher.admin.CypherCreateNamespaceAlias;
import org.polypheny.db.cypher.admin.CypherCreateRole;
import org.polypheny.db.cypher.admin.CypherDenyPrivilege;
import org.polypheny.db.cypher.admin.CypherDropAlias;
import org.polypheny.db.cypher.admin.CypherDropNamespace;
import org.polypheny.db.cypher.admin.CypherGrantPrivilege;
import org.polypheny.db.cypher.admin.CypherRevokePrivilege;
import org.polypheny.db.cypher.admin.CypherShowAllPrivileges;
import org.polypheny.db.cypher.admin.CypherShowCurrentUser;
import org.polypheny.db.cypher.admin.CypherShowNamespace;
import org.polypheny.db.cypher.admin.CypherShowRolePrivileges;
import org.polypheny.db.cypher.admin.CypherShowUserPrivileges;
import org.polypheny.db.cypher.admin.CypherShowUsers;
import org.polypheny.db.cypher.admin.CypherStartNamespace;
import org.polypheny.db.cypher.admin.CypherStopNamespace;
import org.polypheny.db.cypher.admin.CypherWithGraph;
import org.polypheny.db.cypher.clause.CypherCall;
import org.polypheny.db.cypher.clause.CypherCase;
import org.polypheny.db.cypher.clause.CypherClause;
import org.polypheny.db.cypher.clause.CypherCreate;
import org.polypheny.db.cypher.clause.CypherCreateConstraint;
import org.polypheny.db.cypher.clause.CypherDelete;
import org.polypheny.db.cypher.clause.CypherDropConstraint;
import org.polypheny.db.cypher.clause.CypherForeach;
import org.polypheny.db.cypher.clause.CypherLoadCSV;
import org.polypheny.db.cypher.clause.CypherMatch;
import org.polypheny.db.cypher.clause.CypherMerge;
import org.polypheny.db.cypher.clause.CypherOrderItem;
import org.polypheny.db.cypher.clause.CypherQuery;
import org.polypheny.db.cypher.clause.CypherRemove;
import org.polypheny.db.cypher.clause.CypherReturn;
import org.polypheny.db.cypher.clause.CypherReturnClause;
import org.polypheny.db.cypher.clause.CypherReturnItem;
import org.polypheny.db.cypher.clause.CypherReturns;
import org.polypheny.db.cypher.clause.CypherSetClause;
import org.polypheny.db.cypher.clause.CypherSubQuery;
import org.polypheny.db.cypher.clause.CypherTerminateTransaction;
import org.polypheny.db.cypher.clause.CypherUnwind;
import org.polypheny.db.cypher.clause.CypherUseClause;
import org.polypheny.db.cypher.clause.CypherWaitClause;
import org.polypheny.db.cypher.clause.CypherWhere;
import org.polypheny.db.cypher.clause.CypherWith;
import org.polypheny.db.cypher.ddl.CypherAddPlacement;
import org.polypheny.db.cypher.ddl.CypherAlterUser;
import org.polypheny.db.cypher.ddl.CypherCreateIndex;
import org.polypheny.db.cypher.ddl.CypherCreateIndex.IndexType;
import org.polypheny.db.cypher.ddl.CypherCreateUser;
import org.polypheny.db.cypher.ddl.CypherDropIndex;
import org.polypheny.db.cypher.ddl.CypherDropPlacement;
import org.polypheny.db.cypher.ddl.CypherDropRole;
import org.polypheny.db.cypher.ddl.CypherDropUser;
import org.polypheny.db.cypher.ddl.CypherGrantRoles;
import org.polypheny.db.cypher.ddl.CypherRenameRole;
import org.polypheny.db.cypher.ddl.CypherRenameUser;
import org.polypheny.db.cypher.ddl.CypherRevokeRoles;
import org.polypheny.db.cypher.ddl.CypherSchemaCommand;
import org.polypheny.db.cypher.ddl.CypherSetOwnPassword;
import org.polypheny.db.cypher.ddl.CypherShowRoles;
import org.polypheny.db.cypher.expression.CypherAggregate;
import org.polypheny.db.cypher.expression.CypherBinary;
import org.polypheny.db.cypher.expression.CypherComparison;
import org.polypheny.db.cypher.expression.CypherExistSubQuery;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.cypher.expression.CypherExpression.ExpressionType;
import org.polypheny.db.cypher.expression.CypherFormula;
import org.polypheny.db.cypher.expression.CypherFunctionInvocation;
import org.polypheny.db.cypher.expression.CypherGate;
import org.polypheny.db.cypher.expression.CypherGate.Gate;
import org.polypheny.db.cypher.expression.CypherListComprehension;
import org.polypheny.db.cypher.expression.CypherListLookup;
import org.polypheny.db.cypher.expression.CypherListSlice;
import org.polypheny.db.cypher.expression.CypherLiteral;
import org.polypheny.db.cypher.expression.CypherLiteral.Literal;
import org.polypheny.db.cypher.expression.CypherPasswordExpression;
import org.polypheny.db.cypher.expression.CypherPatternComprehension;
import org.polypheny.db.cypher.expression.CypherProperty;
import org.polypheny.db.cypher.expression.CypherReduceExpression;
import org.polypheny.db.cypher.expression.CypherUnary;
import org.polypheny.db.cypher.expression.CypherUseGraph;
import org.polypheny.db.cypher.expression.CypherVariable;
import org.polypheny.db.cypher.hint.CypherHint;
import org.polypheny.db.cypher.hint.CypherIndexHint;
import org.polypheny.db.cypher.hint.CypherJoinHint;
import org.polypheny.db.cypher.hint.CypherScanHint;
import org.polypheny.db.cypher.mapprojection.CypherMPAll;
import org.polypheny.db.cypher.mapprojection.CypherMPItem;
import org.polypheny.db.cypher.mapprojection.CypherMPLiteral;
import org.polypheny.db.cypher.mapprojection.CypherMPProperty;
import org.polypheny.db.cypher.mapprojection.CypherMPVariable;
import org.polypheny.db.cypher.mapprojection.CypherMapProjection;
import org.polypheny.db.cypher.parser.StringPos;
import org.polypheny.db.cypher.pattern.CypherEveryPathPattern;
import org.polypheny.db.cypher.pattern.CypherNamedPattern;
import org.polypheny.db.cypher.pattern.CypherNodePattern;
import org.polypheny.db.cypher.pattern.CypherPattern;
import org.polypheny.db.cypher.pattern.CypherRelPattern;
import org.polypheny.db.cypher.pattern.CypherShortestPathPattern;
import org.polypheny.db.cypher.query.CypherInTransactionParams;
import org.polypheny.db.cypher.query.CypherPeriodicCommit;
import org.polypheny.db.cypher.query.CypherSingleQuery;
import org.polypheny.db.cypher.query.CypherUnion;
import org.polypheny.db.cypher.remove.CypherRemoveItem;
import org.polypheny.db.cypher.remove.CypherRemoveLabels;
import org.polypheny.db.cypher.remove.CypherRemoveProperty;
import org.polypheny.db.cypher.scope.CypherGraphScope;
import org.polypheny.db.cypher.scope.CypherNamespaceScope;
import org.polypheny.db.cypher.set.CypherSetItem;
import org.polypheny.db.cypher.set.CypherSetLabels;
import org.polypheny.db.cypher.set.CypherSetProperty;
import org.polypheny.db.cypher.set.CypherSetVariable;
import org.polypheny.db.cypher.show.CypherShowConstraint;
import org.polypheny.db.cypher.show.CypherShowFunction;
import org.polypheny.db.cypher.show.CypherShowIndex;
import org.polypheny.db.cypher.show.CypherShowProcedure;
import org.polypheny.db.cypher.show.CypherShowTransactions;
import org.polypheny.db.languages.ParserPos;

public interface CypherFactory {

    static ParserPos inputPosition( int beginOffset, int beginLine, int beginColumn ) {
        return new ParserPos( beginLine, beginColumn );
    }

    static CypherQuery createPeriodicCommit( ParserPos pos, ParserPos commitPos, String batchSize, CypherClause loadCsv, List<CypherClause> queryBody ) {
        return new CypherPeriodicCommit( pos, commitPos, batchSize, loadCsv, queryBody );
    }

    static CypherQuery createUnion( ParserPos pos, CypherQuery left, CypherQuery right, boolean all ) {
        return new CypherUnion( pos, left, right, all );
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

    static CypherOrderItem createOrderItem( ParserPos pos, boolean asc, CypherExpression expression ) {
        return new CypherOrderItem( pos, asc, expression );
    }

    static CypherWhere createWhere( ParserPos pos, CypherExpression expression ) {
        return new CypherWhere( pos, expression );
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

    static CypherSetItem createSetLabels( CypherVariable variable, List<StringPos> labels ) {
        return new CypherSetLabels( variable, labels );
    }

    static CypherClause createRemove( ParserPos pos, List<CypherRemoveItem> items ) {
        return new CypherRemove( pos, items );
    }

    static CypherRemoveItem createRemoveProperty( CypherProperty property ) {
        return new CypherRemoveProperty( property );
    }

    static CypherRemoveItem createRemoveLabels( CypherVariable variable, List<StringPos> labels ) {
        return new CypherRemoveLabels( variable, labels );
    }

    static CypherClause createDelete( ParserPos pos, boolean detach, List<CypherExpression> list ) {
        return new CypherDelete( pos, detach, list );
    }

    static CypherClause createMatch( ParserPos pos, boolean optional, List<CypherPattern> patterns, ParserPos pos1, List<CypherHint> hints, CypherWhere where, boolean fullScan ) {
        if ( fullScan ) {
            return new CypherFull( pos );
        }
        return new CypherMatch( pos, optional, patterns, pos1, hints, where, false );
    }

    static CypherHint createIndexHint( ParserPos pos, CypherVariable variable, String labelOrRelType, List<String> propNames, boolean seek, HintIndexType indexType ) {
        return new CypherIndexHint( pos, variable, labelOrRelType, propNames, seek, indexType );
    }

    static CypherClause createMerge( ParserPos pos, CypherPattern pattern, ArrayList<CypherSetClause> clauses, ArrayList<MergeActionType> actionTypes, ArrayList<ParserPos> positions ) {
        return new CypherMerge( pos, pattern, clauses, actionTypes, positions );
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

    static CypherNodePattern createNodePattern( ParserPos pos, CypherVariable variable, List<StringPos> labels, CypherExpression properties, CypherExpression predicate ) {
        return new CypherNodePattern( pos, variable, labels, properties, predicate );
    }

    static CypherExpression createHasLabelOrTypes( CypherExpression subject, List<StringPos> labels ) {
        return new CypherHasLabelOrTypes( subject, labels );
    }

    static CypherRelPattern createRelationshipPattern( ParserPos pos, boolean left, boolean right, CypherVariable variable, List<StringPos> relTypes, CypherPathLength pathLength, CypherExpression properties, CypherExpression predicate, boolean legacyTypeSeparator ) {
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
        return new CypherExpression( pos, ExpressionType.ALL, variable, expression, where );
    }

    static CypherExpression anyExpression( ParserPos pos, CypherVariable variable, CypherExpression expression, CypherExpression where ) {
        return new CypherExpression( pos, ExpressionType.ANY, variable, expression, where );
    }

    static CypherExpression noneExpression( ParserPos pos, CypherVariable variable, CypherExpression expression, CypherExpression where ) {
        return new CypherExpression( pos, ExpressionType.NONE, variable, expression, where );
    }

    static CypherExpression singleExpression( ParserPos pos, CypherVariable variable, CypherExpression expression, CypherExpression where ) {
        return new CypherExpression( pos, ExpressionType.SINGLE, variable, expression, where );
    }

    static CypherExpression patternExpression( ParserPos pos, CypherPattern pattern ) {
        return new CypherExpression( pos, ExpressionType.PATTERN, pattern );
    }

    static CypherExpression mapProjection( ParserPos pos, CypherVariable variable, List<CypherMPItem> items ) {
        return new CypherMapProjection( pos, variable, items );
    }

    static CypherMPItem mapProjectionLiteralEntry( StringPos pos, CypherExpression expression ) {
        return new CypherMPLiteral( pos, expression );
    }

    static CypherMPItem mapProjectionProperty( StringPos pos ) {
        return new CypherMPProperty( pos );
    }

    static CypherMPItem mapProjectionVariable( CypherVariable variable ) {
        return new CypherMPVariable( variable );
    }

    static CypherMPItem mapProjectionAll( ParserPos pos ) {
        return new CypherMPAll( pos );
    }

    static CypherExpression existsSubQuery( ParserPos pos, List<CypherPattern> patterns, CypherExpression where ) {
        return new CypherExistSubQuery( pos, patterns, where );
    }

    static CypherExpression listLiteral( ParserPos pos, List<CypherExpression> list ) {
        return new CypherLiteral( pos, Literal.LIST, list );
    }

    static CypherExpression mapLiteral( ParserPos pos, List<StringPos> keys, List<CypherExpression> values ) {
        return new CypherLiteral( pos, Literal.MAP, keys, values );
    }

    static CypherExpression newString( ParserPos pos, String image ) {
        return new CypherLiteral( pos, Literal.STRING, image );
    }

    static CypherExpression newDouble( ParserPos pos, String image ) {
        return new CypherLiteral( pos, Literal.DOUBLE, image, false );
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

    static CypherSchemaCommand createConstraint( ParserPos pos, ConstraintType constraintType, boolean replace, boolean ifNotExists, String name, CypherVariable variable, StringPos parserPosStringPos, List<CypherProperty> properties, CypherSimpleEither<String, ?> options, boolean containsOn, ConstraintVersion constraintVersion ) {
        return new CypherCreateConstraint( pos, constraintType, replace, ifNotExists, name, variable, parserPosStringPos, properties, options, containsOn, constraintVersion );
    }

    static CypherSchemaCommand dropConstraint( ParserPos pos, ConstraintType constraintType, CypherVariable variable, StringPos parserPosStringPos, List<CypherProperty> properties ) {
        return new CypherDropConstraint( pos, constraintType, variable, parserPosStringPos, properties );
    }

    static CypherSchemaCommand dropConstraint( ParserPos pos, String name, boolean ifExists ) {
        return new CypherDropConstraint( pos, name, ifExists );
    }

    static CypherSchemaCommand createFulltextIndex( ParserPos pos, boolean replace, boolean ifNotExists, boolean isNode, String indexName, CypherVariable variable, List<StringPos> labels, List<CypherProperty> properties, CypherSimpleEither<String, ?> options ) {
        return new CypherCreateIndex( pos, IndexType.FULL_TEXT, replace, ifNotExists, isNode, indexName, variable, labels, properties, options );
    }

    static CypherSchemaCommand createLookupIndex( ParserPos pos, boolean replace, boolean ifNotExists, boolean isNode, String indexName, CypherVariable variable, StringPos funcName, CypherVariable funcParam, CypherSimpleEither<String, ?> options ) {
        return new CypherCreateIndex( pos, IndexType.LOOKUP, replace, ifNotExists, isNode, indexName, variable, funcName, funcParam, options );
    }

    static CypherSchemaCommand dropIndex( ParserPos pos, StringPos stringPos, List<StringPos> properties ) {
        return new CypherDropIndex( pos, stringPos, properties, false );
    }

    static CypherSchemaCommand dropIndex( ParserPos pos, String name, boolean ifExists ) {
        return new CypherDropIndex( pos, name, ifExists );
    }

    static CypherUseClause createUse( ParserPos pos, CypherExpression expression ) {
        return new CypherUseClause( pos, expression );
    }

    static CypherAdminCommand addPlacement( ParserPos pos, CypherSimpleEither<String, CypherParameter> namespaceName, CypherSimpleEither<String, CypherParameter> store ) {
        return new CypherAddPlacement( pos, namespaceName, store );
    }

    static CypherAdminCommand dropPlacement( ParserPos pos, CypherSimpleEither<String, CypherParameter> namespaceName, CypherSimpleEither<String, CypherParameter> store ) {
        return new CypherDropPlacement( pos, namespaceName, store );
    }

    static CypherAdminCommand dropRole( ParserPos pos, CypherSimpleEither<String, CypherParameter> roleName, boolean ifExists ) {
        return new CypherDropRole( pos, roleName, ifExists );
    }

    static CypherAdminCommand renameRole( ParserPos pos, CypherSimpleEither<String, CypherParameter> fromRoleName, CypherSimpleEither<String, CypherParameter> toRoleName, boolean ifExists ) {
        return new CypherRenameRole( pos, fromRoleName, toRoleName, ifExists );
    }

    static CypherWithGraph showRoles( ParserPos pos, boolean withUsers, boolean showAll, CypherYield yield, CypherReturnClause returnClause, CypherWhere where ) {
        return new CypherShowRoles( pos, withUsers, showAll, yield, returnClause, where );
    }

    static CypherAdminCommand grantRoles( ParserPos pos, List<CypherSimpleEither<String, CypherParameter>> roles, List<CypherSimpleEither<String, CypherParameter>> users ) {
        return new CypherGrantRoles( pos, roles, users );
    }

    static CypherAdminCommand revokeRoles( ParserPos pos, List<CypherSimpleEither<String, CypherParameter>> roles, List<CypherSimpleEither<String, CypherParameter>> users ) {
        return new CypherRevokeRoles( pos, roles, users );
    }

    static CypherAdminCommand createUser( ParserPos pos, boolean replace, boolean ifNotExists, CypherSimpleEither<String, CypherParameter> username, CypherExpression password, boolean encrypted, Boolean orElse, Boolean orElse1, CypherSimpleEither<String, CypherParameter> orElse2 ) {
        return new CypherCreateUser( pos, replace, ifNotExists, username, password, encrypted, orElse, orElse1, orElse2 );
    }

    static CypherAdminCommand dropUser( ParserPos pos, boolean ifExists, CypherSimpleEither<String, CypherParameter> username ) {
        return new CypherDropUser( pos, ifExists, username );
    }

    static CypherAdminCommand renameUser( ParserPos pos, CypherSimpleEither<String, CypherParameter> fromUserName, CypherSimpleEither<String, CypherParameter> toUserName, boolean ifExists ) {
        return new CypherRenameUser( pos, fromUserName, toUserName, ifExists );
    }

    static CypherAdminCommand setOwnPassword( ParserPos pos, CypherExpression currentPassword, CypherExpression newPassword ) {
        return new CypherSetOwnPassword( pos, currentPassword, newPassword );
    }

    static CypherAdminCommand alterUser( ParserPos pos, boolean ifExists, CypherSimpleEither<String, CypherParameter> username, CypherExpression password, boolean encrypted, Boolean orElse, Boolean orElse1, CypherSimpleEither<String, CypherParameter> orElse2, boolean removeHome ) {
        return new CypherAlterUser( pos, ifExists, username, password, encrypted, orElse, orElse1, orElse2, removeHome );
    }

    static CypherExpression passwordExpression( ParserPos pos, String password ) {
        return new CypherPasswordExpression( pos, password );
    }

    static CypherExpression passwordExpression( CypherParameter parameter ) {
        return new CypherPasswordExpression( parameter.pos, parameter );
    }

    static CypherWithGraph showUsers( ParserPos pos, CypherYield yield, CypherReturnClause returnClause, CypherWhere where ) {
        return new CypherShowUsers( pos, yield, returnClause, where );
    }

    static CypherWithGraph showCurrentUser( ParserPos pos, CypherYield yield, CypherReturnClause returnClause, CypherWhere where ) {
        return new CypherShowCurrentUser( pos, yield, returnClause, where );
    }

    static CypherWithGraph showAllPrivileges( ParserPos pos, boolean asCommand, boolean asRevoke, CypherYield yield, CypherReturnClause returnClause, CypherWhere where ) {
        return new CypherShowAllPrivileges( pos, asCommand, asRevoke, yield, returnClause, where );
    }

    static CypherWithGraph showRolePrivileges( ParserPos pos, List<CypherSimpleEither<String, CypherParameter>> roles, boolean asCommand, boolean asRevoke, CypherYield yield, CypherReturnClause returnClause, CypherWhere where ) {
        return new CypherShowRolePrivileges( pos, roles, asCommand, asRevoke, yield, returnClause, where );
    }

    static CypherWithGraph showUserPrivileges( ParserPos pos, List<CypherSimpleEither<String, CypherParameter>> users, boolean asCommand, boolean asRevoke, CypherYield yield, CypherReturnClause returnClause, CypherWhere where ) {
        return new CypherShowUserPrivileges( pos, users, asCommand, asRevoke, yield, returnClause, where );
    }

    static CypherAdminCommand grantPrivilege( ParserPos pos, List<CypherSimpleEither<String, CypherParameter>> roles, CypherPrivilegeType privilege ) {
        return new CypherGrantPrivilege( pos, roles, privilege );
    }

    static CypherAdminCommand revokePrivilege( ParserPos pos, List<CypherSimpleEither<String, CypherParameter>> roles, CypherPrivilegeType privilege, boolean revokeGrant, boolean revokeDeny ) {
        return new CypherRevokePrivilege( pos, roles, privilege, revokeGrant, revokeDeny );
    }

    static CypherAdminAction privilegeAction( ActionType type ) {
        return new CypherAdminAction( type );
    }

    static List<CypherPrivilegeQualifier> allQualifier() {
        return ImmutableList.of( new CypherPrivilegeQualifier( ParserPos.ZERO, ImmutableList.of(), CypherPrivilegeQualifier.QualifierType.ALL ) );
    }

    static List<CypherPrivilegeQualifier> allNamespacesQualifier() {
        return ImmutableList.of( new CypherPrivilegeQualifier( ParserPos.ZERO, ImmutableList.of(), CypherPrivilegeQualifier.QualifierType.ALL_NAMESPACES ) );
    }

    static List<CypherPrivilegeQualifier> allUsersQualifier() {
        return ImmutableList.of( new CypherPrivilegeQualifier( ParserPos.ZERO, ImmutableList.of(), CypherPrivilegeQualifier.QualifierType.ALL_USERS ) );
    }

    static CypherGraphScope graphScopes( ParserPos pos, List<CypherSimpleEither<String, CypherParameter>> names, ScopeType scopeType ) {
        return new CypherGraphScope( pos, names, scopeType );
    }

    static CypherPrivilegeType graphPrivilege( ParserPos pos, CypherAdminAction privilegeAction, CypherGraphScope graphScopes, CypherResource resource, List<CypherPrivilegeQualifier> qualifiers ) {
        return new CypherPrivilegeType( pos, Collections.singletonList( privilegeAction ), Collections.singletonList( graphScopes ), resource, qualifiers );
    }

    static CypherPrivilegeType graphPrivilege( ParserPos pos, CypherAdminAction privilegeAction, List<CypherGraphScope> graphs, CypherResource resource, CypherPrivilegeQualifier qualifier ) {
        return graphPrivilege( pos, privilegeAction, graphs, resource, ImmutableList.of( qualifier ) );
    }


    static CypherPrivilegeType namespacePrivilege( ParserPos pos, CypherAdminAction privilegeAction, List<CypherNamespaceScope> namespaceScopes, List<CypherPrivilegeQualifier> qualifiers ) {
        return new CypherPrivilegeType( pos, Collections.singletonList( privilegeAction ), namespaceScopes, null, qualifiers );
    }

    static CypherResource allLabelsResource( ParserPos pos ) {
        return new CypherResource( pos, ResourceType.LABEL );
    }

    static CypherResource labelsResource( ParserPos pos, List<String> names ) {
        return new CypherResource( pos, names, ResourceType.LABEL );
    }

    static CypherResource allPropertiesResource( ParserPos pos ) {
        return new CypherResource( pos, ResourceType.PROPERTIES );
    }

    static CypherResource propertiesResource( ParserPos pos, List<String> names ) {
        return new CypherResource( pos, names, ResourceType.PROPERTIES );
    }

    static CypherPrivilegeQualifier allRelationshipsQualifier( ParserPos pos ) {
        return new CypherPrivilegeQualifier( pos, ImmutableList.of(), CypherPrivilegeQualifier.QualifierType.ALL_RELATIONSHIP );
    }

    static CypherPrivilegeQualifier relationshipQualifier( ParserPos pos, String image ) {
        return new CypherPrivilegeQualifier( pos, Collections.singletonList( image ), CypherPrivilegeQualifier.QualifierType.RELATIONSHIP );
    }

    static CypherPrivilegeQualifier labelQualifier( ParserPos pos, String image ) {
        return new CypherPrivilegeQualifier( pos, ImmutableList.of( image ), CypherPrivilegeQualifier.QualifierType.LABEL );
    }

    static CypherPrivilegeQualifier allElementsQualifier( ParserPos pos ) {
        return new CypherPrivilegeQualifier( pos, ImmutableList.of(), CypherPrivilegeQualifier.QualifierType.ALL_ELEMENTS );
    }

    static CypherPrivilegeQualifier elementQualifier( ParserPos pos, String image ) {
        return new CypherPrivilegeQualifier( pos, ImmutableList.of( image ), CypherPrivilegeQualifier.QualifierType.ELEMENT );
    }

    static CypherWaitClause wait( boolean wait, long nanos ) {
        return new CypherWaitClause( wait, nanos );
    }

    static CypherAdminCommand createNamespace( ParserPos pos, boolean replace, CypherSimpleEither<String, CypherParameter> namespaceName, boolean ifNotExists, CypherWaitClause wait, CypherSimpleEither<String, CypherParameter> options, CypherSimpleEither<String, CypherParameter> store ) {
        return new CypherCreateNamespace( pos, replace, namespaceName, ifNotExists, wait, options, store );
    }

    static CypherAdminCommand dropNamespace( ParserPos pos, CypherSimpleEither<String, CypherParameter> namespaceName, boolean ifExists, boolean dumpData, CypherWaitClause wait ) {
        return new CypherDropNamespace( pos, namespaceName, ifExists, dumpData, wait );
    }

    static CypherAdminCommand alterNamespace( ParserPos pos, CypherSimpleEither<String, CypherParameter> namespaceName, boolean ifExists, AccessType accessType ) {
        return new CypherAlterNamespace( pos, namespaceName, ifExists, accessType );
    }

    static CypherAdminCommand startNamespace( ParserPos pos, CypherSimpleEither<String, CypherParameter> namespaceName, CypherWaitClause wait ) {
        return new CypherStartNamespace( pos, namespaceName, wait );
    }

    static CypherAdminCommand stopNamespace( ParserPos pos, CypherSimpleEither<String, CypherParameter> namespaceName, CypherWaitClause wait ) {
        return new CypherStopNamespace( pos, namespaceName, wait );
    }

    static CypherWithGraph showNamespace( ParserPos pos, CypherNamespaceScope scope, CypherYield yield, CypherReturnClause returnClause, CypherWhere where ) {
        return new CypherShowNamespace( pos, scope, yield, returnClause, where );
    }

    static CypherNamespaceScope namespaceScope( ParserPos pos, CypherSimpleEither<String, CypherParameter> name, boolean isDefault, boolean isHome ) {
        return new CypherNamespaceScope( pos, name, isDefault, isHome, ScopeType.DEFAULT );
    }

    static List<CypherNamespaceScope> namespaceScopes( ParserPos pos, List<CypherSimpleEither<String, CypherParameter>> names, ScopeType type ) {
        return names.stream().map( n -> new CypherNamespaceScope( pos, n, false, false, type ) ).toList();
    }

    static CypherAdminCommand createNamespaceAlias( ParserPos pos, boolean replace, CypherSimpleEither<String, CypherParameter> aliasName, CypherSimpleEither<String, CypherParameter> targetName, boolean ifNotExists ) {
        return new CypherCreateNamespaceAlias( pos, replace, aliasName, targetName, ifNotExists );
    }

    static CypherAdminCommand dropAlias( ParserPos pos, CypherSimpleEither<String, CypherParameter> aliasName, boolean ifExists ) {
        return new CypherDropAlias( pos, aliasName, ifExists );
    }

    static CypherAdminCommand alterNamespaceAlias( ParserPos pos, CypherSimpleEither<String, CypherParameter> aliasName, CypherSimpleEither<String, CypherParameter> targetName, boolean ifExists ) {
        return new CypherAlterNamespaceAlias( pos, aliasName, targetName, ifExists );
    }

    static List<CypherPrivilegeQualifier> functionQualifier( ParserPos pos, List<String> executables ) {
        return executables.stream().map( e -> new CypherPrivilegeQualifier( pos, ImmutableList.of( e ), CypherPrivilegeQualifier.QualifierType.ALL ) ).toList();
    }

    static List<CypherPrivilegeQualifier> procedureQualifier( ParserPos pos, List<String> executables ) {
        return functionQualifier( pos, executables );
    }

    static CypherPrivilegeType dbmsPrivilege( ParserPos pos, CypherAdminAction action, List<CypherPrivilegeQualifier> qualifiers ) {
        return new CypherPrivilegeType( pos, action, qualifiers );
    }

    static CypherPrivilegeType dbmsPrivilege( ParserPos pos, CypherAdminAction action, CypherPrivilegeQualifier qualifier ) {
        return new CypherPrivilegeType( pos, action, Collections.singletonList( qualifier ) );
    }


    static CypherPrivilegeType graphPrivilege( ParserPos pos, CypherAdminAction action, List<CypherGraphScope> graphs, CypherResource resource, List<CypherPrivilegeQualifier> qualifiers ) {
        return new CypherPrivilegeType( pos, ImmutableList.of( action ), graphs, resource, qualifiers );
    }

    static List<CypherPrivilegeQualifier> userQualifier( List<CypherSimpleEither<String, CypherParameter>> qualifiers ) {
        return qualifiers.stream().map( q -> new CypherPrivilegeQualifier( ParserPos.ZERO, ImmutableList.of( q.getLeft() ), CypherPrivilegeQualifier.QualifierType.USER ) ).toList();
    }

    static CypherHint createJoinHint( ParserPos pos, List<CypherVariable> joinVariables ) {
        return new CypherJoinHint( pos, joinVariables );
    }

    static CypherHint createScanHint( ParserPos pos, CypherVariable variable, String image ) {
        return new CypherScanHint( pos, variable, image );
    }

    static CypherClause createUnwind( ParserPos pos, CypherExpression expression, CypherVariable variable ) {
        return new CypherUnwind( pos, expression, variable );
    }

    static CypherClause createSubQuery( ParserPos pos, CypherQuery query, CypherInTransactionParams inTransactionsParams ) {
        return new CypherSubQuery( pos, query, inTransactionsParams );
    }

    static CypherProperty property( CypherExpression subject, StringPos propKeyName ) {
        return new CypherProperty( subject, propKeyName );
    }

    static CypherParameter newParameter( ParserPos pos, CypherVariable variable, ParameterType type ) {
        return new CypherParameter( pos, variable.getName(), type );
    }

    static CypherParameter newParameter( ParserPos pos, String name, ParameterType type ) {
        return new CypherParameter( pos, name, type );
    }


    static CypherPrivilegeQualifier allLabelsQualifier( ParserPos pos ) {
        return new CypherPrivilegeQualifier( pos, ImmutableList.of(), CypherPrivilegeQualifier.QualifierType.ALL_LABELS );
    }

    static CypherSchemaCommand createIndexWithOldSyntax( ParserPos pos, StringPos stringPos, List<StringPos> properties ) {
        return new CypherCreateIndex( pos, IndexType.OLD_SYNTAX, stringPos, properties );
    }

    static CypherSchemaCommand createIndex( ParserPos pos, boolean replace, boolean ifNotExists, boolean isNode, String indexName, CypherVariable variable, StringPos stringPos, List<CypherProperty> properties, CypherSimpleEither<String, ?> options, IndexType indexType ) {
        return new CypherCreateIndex( pos, indexType, replace, ifNotExists, isNode, indexName, variable, ImmutableList.of( stringPos ), properties, options );
    }

    static CypherAdminCommand createRole( ParserPos pos, boolean replace, CypherSimpleEither<String, CypherParameter> roleName, CypherSimpleEither<String, CypherParameter> sourceRoleName, boolean ifNotExists ) {
        return new CypherCreateRole( pos, replace, roleName, sourceRoleName, ifNotExists );
    }

    static CypherAdminCommand denyPrivilege( ParserPos pos, List<CypherSimpleEither<String, CypherParameter>> roles, CypherPrivilegeType privilege ) {
        return new CypherDenyPrivilege( pos, roles, privilege );
    }

    static CypherExpression newAggregate( ParserPos pos, OperatorName op, CypherExpression target, boolean distinct ) {
        return new CypherAggregate( pos, op, target, distinct );
    }

    static CypherMatch createAllMatch( ParserPos pos, CypherWhere where ) {
        return new CypherMatch( pos, false, List.of(), pos, List.of(), where, true );
    }

}

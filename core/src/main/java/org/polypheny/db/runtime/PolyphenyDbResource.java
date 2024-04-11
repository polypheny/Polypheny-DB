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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.runtime;


import static org.polypheny.db.runtime.Resources.BaseMessage;
import static org.polypheny.db.runtime.Resources.ExInst;
import static org.polypheny.db.runtime.Resources.ExInstWithCause;
import static org.polypheny.db.runtime.Resources.Inst;
import static org.polypheny.db.runtime.Resources.Property;

import org.polypheny.db.nodes.validate.ValidatorException;


/**
 * Compiler-checked resources for the Polypheny-DB project.
 */
public interface PolyphenyDbResource {

    @BaseMessage("line {0,number,#}, column {1,number,#}")
    Inst parserContext( int a0, int a1 );

    @BaseMessage("Bang equal ''!='' is not allowed under the current SQL conformance level")
    ExInst<PolyphenyDbException> bangEqualNotAllowed();

    @BaseMessage("Percent remainder ''%'' is not allowed under the current SQL conformance level")
    ExInst<PolyphenyDbException> percentRemainderNotAllowed();

    @BaseMessage("''LIMIT start, count'' is not allowed under the current SQL conformance level")
    ExInst<PolyphenyDbException> limitStartCountNotAllowed();

    @BaseMessage("APPLY operator is not allowed under the current SQL conformance level")
    ExInst<PolyphenyDbException> applyNotAllowed();

    @BaseMessage("Illegal {0} literal {1}: {2}")
    ExInst<PolyphenyDbException> illegalLiteral( String a0, String a1, String a2 );

    @BaseMessage("Length of identifier ''{0}'' must be less than or equal to {1,number,#} characters")
    ExInst<PolyphenyDbException> identifierTooLong( String a0, int a1 );

    @BaseMessage("not in format ''{0}''")
    Inst badFormat( String a0 );

    @BaseMessage("BETWEEN operator has no terminating AND")
    ExInst<ValidatorException> betweenWithoutAnd();

    @BaseMessage("Geo-spatial extensions and the GEOMETRY data type are not enabled")
    ExInst<ValidatorException> geometryDisabled();

    @BaseMessage("Illegal INTERVAL literal {0}; at {1}")
    @Property(name = "SQLSTATE", value = "42000")
    ExInst<PolyphenyDbException> illegalIntervalLiteral( String a0, String a1 );

    @BaseMessage("Illegal expression. Was expecting \"(DATETIME - DATETIME) INTERVALQUALIFIER\"")
    ExInst<PolyphenyDbException> illegalMinusDate();

    @BaseMessage("Illegal overlaps expression. Was expecting expression on the form \"(DATETIME, EXPRESSION) OVERLAPS (DATETIME, EXPRESSION)\"")
    ExInst<PolyphenyDbException> illegalOverlaps();

    @BaseMessage("Non-query expression encountered in illegal context")
    ExInst<PolyphenyDbException> illegalNonQueryExpression();

    @BaseMessage("Query expression encountered in illegal context")
    ExInst<PolyphenyDbException> illegalQueryExpression();

    @BaseMessage("CURSOR expression encountered in illegal context")
    ExInst<PolyphenyDbException> illegalCursorExpression();

    @BaseMessage("ORDER BY unexpected")
    ExInst<PolyphenyDbException> illegalOrderBy();

    @BaseMessage("Illegal binary string {0}")
    ExInst<PolyphenyDbException> illegalBinaryString( String a0 );

    @BaseMessage("''FROM'' without operands preceding it is illegal")
    ExInst<PolyphenyDbException> illegalFromEmpty();

    @BaseMessage("ROW expression encountered in illegal context")
    ExInst<PolyphenyDbException> illegalRowExpression();

    @BaseMessage("Illegal identifier '':''. Was expecting ''VALUE''")
    ExInst<PolyphenyDbException> illegalColon();

    @BaseMessage("TABLESAMPLE percentage must be between 0 and 100, inclusive")
    @Property(name = "SQLSTATE", value = "2202H")
    ExInst<PolyphenyDbException> invalidSampleSize();

    @BaseMessage("Unknown character set ''{0}''")
    ExInst<PolyphenyDbException> unknownCharacterSet( String a0 );

    @BaseMessage("Failed to encode ''{0}'' in character set ''{1}''")
    ExInst<PolyphenyDbException> charsetEncoding( String a0, String a1 );

    @BaseMessage("UESCAPE ''{0}'' must be exactly one character")
    ExInst<PolyphenyDbException> unicodeEscapeCharLength( String a0 );

    @BaseMessage("UESCAPE ''{0}'' may not be hex digit, whitespace, plus sign, or double quote")
    ExInst<PolyphenyDbException> unicodeEscapeCharIllegal( String a0 );

    @BaseMessage("UESCAPE cannot be specified without Unicode literal introducer")
    ExInst<PolyphenyDbException> unicodeEscapeUnexpected();

    @BaseMessage("Unicode escape sequence starting at character {0,number,#} is not exactly four hex digits")
    ExInst<ValidatorException> unicodeEscapeMalformed( int a0 );

    @BaseMessage("No match found for function signature {0}")
    ExInst<ValidatorException> validatorUnknownFunction( String a0 );

    @BaseMessage("Invalid number of arguments to function ''{0}''. Was expecting {1,number,#} arguments")
    ExInst<ValidatorException> invalidArgCount( String a0, int a1 );

    @BaseMessage("At line {0,number,#}, column {1,number,#}")
    ExInstWithCause<PolyphenyDbContextException> validatorContextPoint( int a0, int a1 );

    @BaseMessage("From line {0,number,#}, column {1,number,#} to line {2,number,#}, column {3,number,#}")
    ExInstWithCause<PolyphenyDbContextException> validatorContext( int a0, int a1, int a2, int a3 );

    @BaseMessage("Cast function cannot convert value of type {0} to type {1}")
    ExInst<ValidatorException> cannotCastValue( String a0, String a1 );

    @BaseMessage("Unknown datatype name ''{0}''")
    ExInst<ValidatorException> unknownDatatypeName( String a0 );

    @BaseMessage("Values passed to {0} operator must have compatible types")
    ExInst<ValidatorException> incompatibleValueType( String a0 );

    @BaseMessage("Values in expression list must have compatible types")
    ExInst<ValidatorException> incompatibleTypesInList();

    @BaseMessage("Cannot apply {0} to the two different charsets {1} and {2}")
    ExInst<ValidatorException> incompatibleCharset( String a0, String a1, String a2 );

    @BaseMessage("ORDER BY is only allowed on top-level SELECT")
    ExInst<ValidatorException> invalidOrderByPos();

    @BaseMessage("Unknown identifier ''{0}''")
    ExInst<ValidatorException> unknownIdentifier( String a0 );

    @BaseMessage("Unknown field ''{0}''")
    ExInst<ValidatorException> unknownField( String a0 );

    @BaseMessage("Unknown target column ''{0}''")
    ExInst<ValidatorException> unknownTargetColumn( String a0 );

    @BaseMessage("Target column ''{0}'' is assigned more than once")
    ExInst<ValidatorException> duplicateTargetColumn( String a0 );

    @BaseMessage("Number of INSERT target columns ({0,number}) does not equal number of source items ({1,number})")
    ExInst<ValidatorException> unmatchInsertColumn( int a0, int a1 );

    @BaseMessage("Column ''{0}'' has no default value and does not allow NULLs")
    ExInst<ValidatorException> columnNotNullable( String a0 );

    @BaseMessage("Cannot assign to target field ''{0}'' of type {1} from source field ''{2}'' of type {3}")
    ExInst<ValidatorException> typeNotAssignable( String a0, String a1, String a2, String a3 );

    @BaseMessage("Array in column ''{0}'' with cardinality {1,number} exceeds max-cardinality of {2,number}")
    ExInst<ValidatorException> exceededCardinality( String a0, long a1, long a2 );

    @BaseMessage("Array in column ''{0}'' with dimension {1,number} exceeds max-dimension of {2,number}")
    ExInst<ValidatorException> exceededDimension( String a0, long a1, long a2 );

    @BaseMessage("Database ''{0}'' not found")
    ExInst<ValidatorException> databaseNotFound( String a0 );

    @BaseMessage("Entity ''{0}'' not found")
    ExInst<ValidatorException> entityNameNotFound( String a0 );

    @BaseMessage("Entity ''{0}'' not found; did you mean ''{1}''?")
    ExInst<ValidatorException> entityNameNotFoundDidYouMean( String a0, String a1 );

    @BaseMessage("Value ''{0}'' is not valid JSON; {1}")
    ExInst<ValidatorException> notValidJson( String a0, String a1 );

    /**
     * Same message as {@link #entityNameNotFound(String)} but a different kind of exception, so it can be used in {@code AlgBuilder}.
     */
    @BaseMessage("Entity ''{0}'' not found")
    ExInst<PolyphenyDbException> entityNotFound( String tableName );

    @BaseMessage("Object ''{0}'' not found")
    ExInst<ValidatorException> objectNotFound( String a0 );

    @BaseMessage("Object ''{0}'' not found within ''{1}''")
    ExInst<ValidatorException> objectNotFoundWithin( String a0, String a1 );

    @BaseMessage("Object ''{0}'' not found; did you mean ''{1}''?")
    ExInst<ValidatorException> objectNotFoundDidYouMean( String a0, String a1 );

    @BaseMessage("Object ''{0}'' not found within ''{1}''; did you mean ''{2}''?")
    ExInst<ValidatorException> objectNotFoundWithinDidYouMean( String a0, String a1, String a2 );

    @BaseMessage("Entity ''{0}'' is not a sequence")
    ExInst<ValidatorException> notASequence( String a0 );

    @BaseMessage("Field ''{0}'' not found in any entity")
    ExInst<ValidatorException> fieldNotFound( String a0 );

    @BaseMessage("Field ''{0}'' not found in any entity; did you mean ''{1}''?")
    ExInst<ValidatorException> fieldNotFoundDidYouMean( String a0, String a1 );

    @BaseMessage("Field ''{0}'' not found in entity ''{1}''")
    ExInst<ValidatorException> fieldNotFoundInEntity( String a0, String a1 );

    @BaseMessage("Field ''{0}'' not found in entity ''{1}''; did you mean ''{2}''?")
    ExInst<ValidatorException> fieldNotFoundInEntityDidYouMean( String a0, String a1, String a2 );

    @BaseMessage("Field ''{0}'' is ambiguous")
    ExInst<ValidatorException> columnAmbiguous( String a0 );

    @BaseMessage("Operand {0} must be a query")
    ExInst<ValidatorException> needQueryOp( String a0 );

    @BaseMessage("Parameters must be of the same type")
    ExInst<ValidatorException> needSameTypeParameter();

    @BaseMessage("Cannot apply ''{0}'' to arguments of type {1}. Supported form(s): {2}")
    ExInst<ValidatorException> canNotApplyOp2Type( String a0, String a1, String a2 );

    @BaseMessage("Expected a boolean type")
    ExInst<ValidatorException> expectedBoolean();

    @BaseMessage("Expected a character type")
    ExInst<ValidatorException> expectedCharacter();

    @BaseMessage("Expected a multimedia type")
    ExInst<ValidatorException> expectedMultimedia();

    @BaseMessage("ELSE clause or at least one THEN clause must be non-NULL")
    ExInst<ValidatorException> mustNotNullInElse();

    @BaseMessage("Function ''{0}'' is not defined")
    ExInst<ValidatorException> functionUndefined( String a0 );

    @BaseMessage("Encountered {0} with {1,number} parameter(s); was expecting {2}")
    ExInst<ValidatorException> wrongNumberOfParam( String a0, int a1, String a2 );

    @BaseMessage("Illegal mixing of types in CASE or COALESCE statement")
    ExInst<ValidatorException> illegalMixingOfTypes();

    @BaseMessage("Invalid compare. Comparing (collation, coercibility): ({0}, {1} with ({2}, {3}) is illegal")
    ExInst<PolyphenyDbException> invalidCompare( String a0, String a1, String a2, String a3 );

    @BaseMessage("Invalid syntax. Two explicit different collations ({0}, {1}) are illegal")
    ExInst<PolyphenyDbException> differentCollations( String a0, String a1 );

    @BaseMessage("{0} is not comparable to {1}")
    ExInst<ValidatorException> typeNotComparable( String a0, String a1 );

    @BaseMessage("Cannot compare values of types ''{0}'', ''{1}''")
    ExInst<ValidatorException> typeNotComparableNear( String a0, String a1 );

    @BaseMessage("Wrong number of arguments to expression")
    ExInst<ValidatorException> wrongNumOfArguments();

    @BaseMessage("Operands {0} not comparable to each other")
    ExInst<ValidatorException> operandNotComparable( String a0 );

    @BaseMessage("Types {0} not comparable to each other")
    ExInst<ValidatorException> typeNotComparableEachOther( String a0 );

    @BaseMessage("Numeric literal ''{0}'' out of range")
    ExInst<ValidatorException> numberLiteralOutOfRange( String a0 );

    @BaseMessage("Date literal ''{0}'' out of range")
    ExInst<ValidatorException> dateLiteralOutOfRange( String a0 );

    @BaseMessage("String literal continued on same line")
    ExInst<ValidatorException> stringFragsOnSameLine();

    @BaseMessage("Entity or field alias must be a simple identifier")
    ExInst<ValidatorException> aliasMustBeSimpleIdentifier();

    @BaseMessage("List of field aliases must have same degree as entity; entity has {0,number,#} fields {1}, whereas alias list has {2,number,#} fields")
    ExInst<ValidatorException> aliasListDegree( int a0, String a1, int a2 );

    @BaseMessage("Duplicate name ''{0}'' in field alias list")
    ExInst<ValidatorException> aliasListDuplicate( String a0 );

    @BaseMessage("INNER, LEFT, RIGHT or FULL join requires a condition (NATURAL keyword or ON or USING clause)")
    ExInst<ValidatorException> joinRequiresCondition();

    @BaseMessage("Cannot specify condition (NATURAL keyword, or ON or USING clause) following CROSS JOIN")
    ExInst<ValidatorException> crossJoinDisallowsCondition();

    @BaseMessage("Cannot specify NATURAL keyword with ON or USING clause")
    ExInst<ValidatorException> naturalDisallowsOnOrUsing();

    @BaseMessage("Field name ''{0}'' in USING clause is not unique on one side of join")
    ExInst<ValidatorException> columnInUsingNotUnique( String a0 );

    @BaseMessage("Field ''{0}'' matched using NATURAL keyword or USING clause has incompatible types: cannot compare ''{1}'' to ''{2}''")
    ExInst<ValidatorException> naturalOrUsingColumnNotCompatible( String a0, String a1, String a2 );

    @BaseMessage("OVER clause is necessary for window functions")
    ExInst<ValidatorException> absentOverClause();

    @BaseMessage("Window ''{0}'' not found")
    ExInst<ValidatorException> windowNotFound( String a0 );

    @BaseMessage("Expression ''{0}'' is not being grouped")
    ExInst<ValidatorException> notGroupExpr( String a0 );

    @BaseMessage("Argument to {0} operator must be a grouped expression")
    ExInst<ValidatorException> groupingArgument( String a0 );

    @BaseMessage("{0} operator may only occur in an aggregate query")
    ExInst<ValidatorException> groupingInAggregate( String a0 );

    @BaseMessage("{0} operator may only occur in SELECT, HAVING or ORDER BY clause")
    ExInst<ValidatorException> groupingInWrongClause( String a0 );

    @BaseMessage("Expression ''{0}'' is not in the select clause")
    ExInst<ValidatorException> notSelectDistinctExpr( String a0 );

    @BaseMessage("Aggregate expression is illegal in {0} clause")
    ExInst<ValidatorException> aggregateIllegalInClause( String a0 );

    @BaseMessage("Windowed aggregate expression is illegal in {0} clause")
    ExInst<ValidatorException> windowedAggregateIllegalInClause( String a0 );

    @BaseMessage("Aggregate expressions cannot be nested")
    ExInst<ValidatorException> nestedAggIllegal();

    @BaseMessage("FILTER must not contain aggregate expression")
    ExInst<ValidatorException> aggregateInFilterIllegal();

    @BaseMessage("WITHIN GROUP must not contain aggregate expression")
    ExInst<ValidatorException> aggregateInWithinGroupIllegal();

    @BaseMessage("Aggregate expression ''{0}'' must contain a within group clause")
    ExInst<ValidatorException> aggregateMissingWithinGroupClause( String a0 );

    @BaseMessage("Aggregate expression ''{0}'' must not contain a within group clause")
    ExInst<ValidatorException> withinGroupClauseIllegalInAggregate( String a0 );

    @BaseMessage("Aggregate expression is illegal in ORDER BY clause of non-aggregating SELECT")
    ExInst<ValidatorException> aggregateIllegalInOrderBy();

    @BaseMessage("{0} clause must be a condition")
    ExInst<ValidatorException> condMustBeBoolean( String a0 );

    @BaseMessage("HAVING clause must be a condition")
    ExInst<ValidatorException> havingMustBeBoolean();

    @BaseMessage("OVER must be applied to aggregate function")
    ExInst<ValidatorException> overNonAggregate();

    @BaseMessage("FILTER must be applied to aggregate function")
    ExInst<ValidatorException> filterNonAggregate();

    @BaseMessage("Cannot override window attribute")
    ExInst<ValidatorException> cannotOverrideWindowAttribute();

    @BaseMessage("Column count mismatch in {0}")
    ExInst<ValidatorException> columnCountMismatchInSetop( String a0 );

    @BaseMessage("Type mismatch in column {0,number} of {1}")
    ExInst<ValidatorException> columnTypeMismatchInSetop( int a0, String a1 );

    @BaseMessage("Binary literal string must contain an even number of hexits")
    ExInst<ValidatorException> binaryLiteralOdd();

    @BaseMessage("Binary literal string must contain only characters ''0'' - ''9'', ''A'' - ''F''")
    ExInst<ValidatorException> binaryLiteralInvalid();

    @BaseMessage("Illegal interval literal format {0} for {1}")
    ExInst<ValidatorException> unsupportedIntervalLiteral( String a0, String a1 );

    @BaseMessage("Interval field value {0,number} exceeds precision of {1} field")
    ExInst<ValidatorException> intervalFieldExceedsPrecision( Number a0, String a1 );

    @BaseMessage("RANGE clause cannot be used with compound ORDER BY clause")
    ExInst<ValidatorException> compoundOrderByProhibitsRange();

    @BaseMessage("Data type of ORDER BY prohibits use of RANGE clause")
    ExInst<ValidatorException> orderByDataTypeProhibitsRange();

    @BaseMessage("Data Type mismatch between ORDER BY and RANGE clause")
    ExInst<ValidatorException> orderByRangeMismatch();

    @BaseMessage("Window ORDER BY expression of type DATE requires range of type INTERVAL")
    ExInst<ValidatorException> dateRequiresInterval();

    @BaseMessage("ROWS value must be a non-negative integral constant")
    ExInst<ValidatorException> rowMustBeNonNegativeIntegral();

    @BaseMessage("Window specification must contain an ORDER BY clause")
    ExInst<ValidatorException> overMissingOrderBy();

    @BaseMessage("PARTITION BY expression should not contain OVER clause")
    ExInst<ValidatorException> partitionbyShouldNotContainOver();

    @BaseMessage("ORDER BY expression should not contain OVER clause")
    ExInst<ValidatorException> orderbyShouldNotContainOver();

    @BaseMessage("UNBOUNDED FOLLOWING cannot be specified for the lower frame boundary")
    ExInst<ValidatorException> badLowerBoundary();

    @BaseMessage("UNBOUNDED PRECEDING cannot be specified for the upper frame boundary")
    ExInst<ValidatorException> badUpperBoundary();

    @BaseMessage("Upper frame boundary cannot be PRECEDING when lower boundary is CURRENT ROW")
    ExInst<ValidatorException> currentRowPrecedingError();

    @BaseMessage("Upper frame boundary cannot be CURRENT ROW when lower boundary is FOLLOWING")
    ExInst<ValidatorException> currentRowFollowingError();

    @BaseMessage("Upper frame boundary cannot be PRECEDING when lower boundary is FOLLOWING")
    ExInst<ValidatorException> followingBeforePrecedingError();

    @BaseMessage("Window name must be a simple identifier")
    ExInst<ValidatorException> windowNameMustBeSimple();

    @BaseMessage("Duplicate window names not allowed")
    ExInst<ValidatorException> duplicateWindowName();

    @BaseMessage("Empty window specification not allowed")
    ExInst<ValidatorException> emptyWindowSpec();

    @BaseMessage("Duplicate window specification not allowed in the same window clause")
    ExInst<ValidatorException> dupWindowSpec();

    @BaseMessage("ROW/RANGE not allowed with RANK, DENSE_RANK or ROW_NUMBER functions")
    ExInst<ValidatorException> rankWithFrame();

    @BaseMessage("RANK or DENSE_RANK functions require ORDER BY clause in window specification")
    ExInst<ValidatorException> funcNeedsOrderBy();

    @BaseMessage("PARTITION BY not allowed with existing window reference")
    ExInst<ValidatorException> partitionNotAllowed();

    @BaseMessage("ORDER BY not allowed in both base and referenced windows")
    ExInst<ValidatorException> orderByOverlap();

    @BaseMessage("Referenced window cannot have framing declarations")
    ExInst<ValidatorException> refWindowWithFrame();

    @BaseMessage("Type ''{0}'' is not supported")
    ExInst<ValidatorException> typeNotSupported( String a0 );

    @BaseMessage("DISTINCT/ALL not allowed with {0} function")
    ExInst<ValidatorException> functionQuantifierNotAllowed( String a0 );

    @BaseMessage("WITHIN GROUP not allowed with {0} function")
    ExInst<ValidatorException> withinGroupNotAllowed( String a0 );

    @BaseMessage("Some but not all arguments are named")
    ExInst<ValidatorException> someButNotAllArgumentsAreNamed();

    @BaseMessage("Duplicate argument name ''{0}''")
    ExInst<ValidatorException> duplicateArgumentName( String name );

    @BaseMessage("DEFAULT is only allowed for optional parameters")
    ExInst<ValidatorException> defaultForOptionalParameter();

    @BaseMessage("DEFAULT not allowed here")
    ExInst<ValidatorException> defaultNotAllowed();

    @BaseMessage("Not allowed to perform {0} on {1}")
    ExInst<ValidatorException> accessNotAllowed( String a0, String a1 );

    @BaseMessage("The {0} function does not support the {1} data type.")
    ExInst<ValidatorException> minMaxBadType( String a0, String a1 );

    @BaseMessage("Only scalar sub-queries allowed in select list.")
    ExInst<ValidatorException> onlyScalarSubQueryAllowed();

    @BaseMessage("Ordinal out of range")
    ExInst<ValidatorException> orderByOrdinalOutOfRange();

    @BaseMessage("Window has negative size")
    ExInst<ValidatorException> windowHasNegativeSize();

    @BaseMessage("UNBOUNDED FOLLOWING window not supported")
    ExInst<ValidatorException> unboundedFollowingWindowNotSupported();

    @BaseMessage("Cannot use DISALLOW PARTIAL with window based on RANGE")
    ExInst<ValidatorException> cannotUseDisallowPartialWithRange();

    @BaseMessage("Interval leading field precision ''{0,number,#}'' out of range for {1}")
    ExInst<ValidatorException> intervalStartPrecisionOutOfRange( int a0, String a1 );

    @BaseMessage("Interval fractional second precision ''{0,number,#}'' out of range for {1}")
    ExInst<ValidatorException> intervalFractionalSecondPrecisionOutOfRange( int a0, String a1 );

    @BaseMessage("Duplicate relation name ''{0}'' in FROM clause")
    ExInst<ValidatorException> fromAliasDuplicate( String a0 );

    @BaseMessage("Duplicate column name ''{0}'' in output")
    ExInst<ValidatorException> duplicateColumnName( String a0 );

    @BaseMessage("Duplicate name ''{0}'' in column list")
    ExInst<ValidatorException> duplicateNameInColumnList( String a0 );

    @BaseMessage("Internal error: {0}")
    ExInst<PolyphenyDbException> internal( String a0 );

    @BaseMessage("Argument to function ''{0}'' must be a literal")
    ExInst<ValidatorException> argumentMustBeLiteral( String a0 );

    @BaseMessage("Argument to function ''{0}'' must be a positive integer literal")
    ExInst<ValidatorException> argumentMustBePositiveInteger( String a0 );

    @BaseMessage("Validation Error: {0}")
    ExInst<PolyphenyDbException> validationError( String a0 );

    @BaseMessage("Locale ''{0}'' in an illegal format")
    ExInst<PolyphenyDbException> illegalLocaleFormat( String a0 );

    @BaseMessage("Argument to function ''{0}'' must not be NULL")
    ExInst<ValidatorException> argumentMustNotBeNull( String a0 );

    @BaseMessage("Illegal use of ''NULL''")
    ExInst<ValidatorException> nullIllegal();

    @BaseMessage("Illegal use of dynamic parameter")
    ExInst<ValidatorException> dynamicParamIllegal();

    @BaseMessage("''{0}'' is not a valid boolean value")
    ExInst<PolyphenyDbException> invalidBoolean( String a0 );

    @BaseMessage("Argument to function ''{0}'' must be a valid precision between ''{1,number,#}'' and ''{2,number,#}''")
    ExInst<ValidatorException> argumentMustBeValidPrecision( String a0, int a1, int a2 );

    @BaseMessage("Wrong arguments for entity function ''{0}'' call. Expected ''{1}'', actual ''{2}''")
    ExInst<PolyphenyDbException> illegalArgumentForTableFunctionCall( String a0, String a1, String a2 );

    @BaseMessage("''{0}'' is not a valid datetime format")
    ExInst<PolyphenyDbException> invalidDatetimeFormat( String a0 );

    @BaseMessage("Cannot INSERT into generated column ''{0}''")
    ExInst<ValidatorException> insertIntoAlwaysGenerated( String a0 );

    @BaseMessage("Argument to function ''{0}'' must have a scale of 0")
    ExInst<PolyphenyDbException> argumentMustHaveScaleZero( String a0 );

    @BaseMessage("Statement preparation aborted")
    ExInst<PolyphenyDbException> preparationAborted();

    @BaseMessage("SELECT DISTINCT not supported")
    @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
    Feature sQLFeature_E051_01();

    @BaseMessage("EXCEPT not supported")
    @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
    Feature sQLFeature_E071_03();

    @BaseMessage("UPDATE not supported")
    @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
    Feature sQLFeature_E101_03();

    @BaseMessage("Transactions not supported")
    @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
    Feature sQLFeature_E151();

    @BaseMessage("INTERSECT not supported")
    @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
    Feature sQLFeature_F302();

    @BaseMessage("MERGE not supported")
    @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
    Feature sQLFeature_F312();

    @BaseMessage("Basic multiset not supported")
    @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
    Feature sQLFeature_S271();

    @BaseMessage("TABLESAMPLE not supported")
    @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
    Feature sQLFeature_T613();

    @BaseMessage("Execution of a new autocommit statement while a cursor is still open on the same connection is not supported")
    @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
    ExInst<PolyphenyDbException> sQLConformance_MultipleActiveAutocommitStatements();

    @BaseMessage("Descending sort (ORDER BY DESC) not supported")
    @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
    Feature sQLConformance_OrderByDesc();

    @BaseMessage("Sharing of cached statement plans not supported")
    @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
    ExInst<PolyphenyDbException> sharedStatementPlans();

    @BaseMessage("TABLESAMPLE SUBSTITUTE not supported")
    @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
    Feature sQLFeatureExt_T613_Substitution();

    @BaseMessage("Personality does not maintain entity''s row count in the catalog")
    @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
    ExInst<PolyphenyDbException> personalityManagesRowCount();

    @BaseMessage("Personality does not support snapshot reads")
    @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
    ExInst<PolyphenyDbException> personalitySupportsSnapshots();

    @BaseMessage("Personality does not support labels")
    @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
    ExInst<PolyphenyDbException> personalitySupportsLabels();

    @BaseMessage("Require at least 1 argument")
    ExInst<ValidatorException> requireAtLeastOneArg();

    @BaseMessage("Map requires at least 2 arguments")
    ExInst<ValidatorException> mapRequiresTwoOrMoreArgs();

    @BaseMessage("Map requires an even number of arguments")
    ExInst<ValidatorException> mapRequiresEvenArgCount();

    @BaseMessage("Incompatible types")
    ExInst<ValidatorException> incompatibleTypes();

    @BaseMessage("Number of columns must match number of query columns")
    ExInst<ValidatorException> columnCountMismatch();

    @BaseMessage("Column has duplicate column name ''{0}'' and no column list specified")
    ExInst<ValidatorException> duplicateColumnAndNoColumnList( String s );

    @BaseMessage("Declaring class ''{0}'' of non-static user-defined function must have a public constructor with zero parameters")
    ExInst<RuntimeException> requireDefaultConstructor( String className );

    @BaseMessage("In user-defined aggregate class ''{0}'', first parameter to ''add'' method must be the accumulator (the return type of the ''init'' method)")
    ExInst<RuntimeException> firstParameterOfAdd( String className );

    @BaseMessage("FilterableEntity.scan returned a filter that was not in the original list: {0}")
    ExInst<PolyphenyDbException> filterableEntityInventedFilter( String s );

    @BaseMessage("FilterableEntity.scan must not return null")
    ExInst<PolyphenyDbException> filterableScanReturnedNull();

    @BaseMessage("Cannot convert entity ''{0}'' to stream")
    ExInst<ValidatorException> cannotConvertToStream( String tableName );

    @BaseMessage("Cannot convert stream ''{0}'' to relation")
    ExInst<ValidatorException> cannotConvertToRelation( String tableName );

    @BaseMessage("Streaming aggregation requires at least one monotonic expression in GROUP BY clause")
    ExInst<ValidatorException> streamMustGroupByMonotonic();

    @BaseMessage("Streaming ORDER BY must start with monotonic expression")
    ExInst<ValidatorException> streamMustOrderByMonotonic();

    @BaseMessage("Set operator cannot combine streaming and non-streaming inputs")
    ExInst<ValidatorException> streamSetOpInconsistentInputs();

    @BaseMessage("Cannot stream VALUES")
    ExInst<ValidatorException> cannotStreamValues();

    @BaseMessage("Cannot resolve ''{0}''; it references view ''{1}'', whose definition is cyclic")
    ExInst<ValidatorException> cyclicDefinition( String id, String view );

    @BaseMessage("Modifiable view must be based on a single entity")
    ExInst<ValidatorException> modifiableViewMustBeBasedOnSingleTable();

    @BaseMessage("Modifiable view must be predicated only on equality expressions")
    ExInst<ValidatorException> modifiableViewMustHaveOnlyEqualityPredicates();

    @BaseMessage("View is not modifiable. More than one expression maps to column ''{0}'' of base entity ''{1}''")
    ExInst<ValidatorException> moreThanOneMappedColumn( String columnName, String tableName );

    @BaseMessage("View is not modifiable. No value is supplied for NOT NULL column ''{0}'' of base entity ''{1}''")
    ExInst<ValidatorException> noValueSuppliedForViewColumn( String columnName, String tableName );

    @BaseMessage("Modifiable view constraint is not satisfied for column ''{0}'' of base entity ''{1}''")
    ExInst<ValidatorException> viewConstraintNotSatisfied( String columnName, String tableName );

    @BaseMessage("Not a record type. The ''*'' operator requires a record")
    ExInst<ValidatorException> starRequiresRecordType();

    @BaseMessage("FILTER expression must be of type BOOLEAN")
    ExInst<PolyphenyDbException> filterMustBeBoolean();

    @BaseMessage("Cannot stream results of a query with no streaming inputs: ''{0}''. At least one input should be convertible to a stream")
    ExInst<ValidatorException> cannotStreamResultsForNonStreamingInputs( String inputs );

    @BaseMessage("MINUS is not allowed under the current SQL conformance level")
    ExInst<PolyphenyDbException> minusNotAllowed();

    @BaseMessage("SELECT must have a FROM clause")
    ExInst<ValidatorException> selectMissingFrom();

    @BaseMessage("Group function ''{0}'' can only appear in GROUP BY clause")
    ExInst<ValidatorException> groupFunctionMustAppearInGroupByClause( String funcName );

    @BaseMessage("Call to auxiliary group function ''{0}'' must have matching call to group function ''{1}'' in GROUP BY clause")
    ExInst<ValidatorException> auxiliaryWithoutMatchingGroupCall( String func1, String func2 );

    @BaseMessage("Pattern variable ''{0}'' has already been defined")
    ExInst<ValidatorException> patternVarAlreadyDefined( String varName );

    @BaseMessage("Cannot use PREV/NEXT in MEASURE ''{0}''")
    ExInst<ValidatorException> patternPrevFunctionInMeasure( String call );

    @BaseMessage("Cannot nest PREV/NEXT under LAST/FIRST ''{0}''")
    ExInst<ValidatorException> patternPrevFunctionOrder( String call );

    @BaseMessage("Cannot use aggregation in navigation ''{0}''")
    ExInst<ValidatorException> patternAggregationInNavigation( String call );

    @BaseMessage("Invalid number of parameters to COUNT method")
    ExInst<ValidatorException> patternCountFunctionArg();

    @BaseMessage("Cannot use RUNNING/FINAL in DEFINE ''{0}''")
    ExInst<ValidatorException> patternRunningFunctionInDefine( String call );

    @BaseMessage("Multiple pattern variables in ''{0}''")
    ExInst<ValidatorException> patternFunctionVariableCheck( String call );

    @BaseMessage("Function ''{0}'' can only be used in MATCH_RECOGNIZE")
    ExInst<ValidatorException> functionMatchRecognizeOnly( String call );

    @BaseMessage("Null parameters in ''{0}''")
    ExInst<ValidatorException> patternFunctionNullCheck( String call );

    @BaseMessage("Unknown pattern ''{0}''")
    ExInst<ValidatorException> unknownPattern( String call );

    @BaseMessage("Interval must be non-negative ''{0}''")
    ExInst<ValidatorException> intervalMustBeNonNegative( String call );

    @BaseMessage("Must contain an ORDER BY clause when WITHIN is used")
    ExInst<ValidatorException> cannotUseWithinWithoutOrderBy();

    @BaseMessage("First field of ORDER BY must be of type TIMESTAMP")
    ExInst<ValidatorException> firstColumnOfOrderByMustBeTimestamp();

    @BaseMessage("Extended fields not allowed under the current SQL conformance level")
    ExInst<ValidatorException> extendNotAllowed();

    @BaseMessage("Rolled up field ''{0}'' is not allowed in {1}")
    ExInst<ValidatorException> rolledUpNotAllowed( String column, String context );

    @BaseMessage("Namespace ''{0}'' already exists")
    ExInst<ValidatorException> schemaExists( String name );

    @BaseMessage("Field ''{0}'' already exists")
    ExInst<ValidatorException> columnExists( String name );

    @BaseMessage("Field ''{0}'' is defined NOT NULL and has no default value assigned")
    ExInst<ValidatorException> notNullAndNoDefaultValue( String name );

    @BaseMessage("Invalid namespace type ''{0}''; valid values: {1}")
    ExInst<ValidatorException> schemaInvalidType( String type, String values );

    @BaseMessage("Entity ''{0}'' already exists")
    ExInst<ValidatorException> tableExists( String name );

    // If CREATE TABLE does not have "AS query", there must be a column list
    @BaseMessage("Missing field list")
    ExInst<ValidatorException> createTableRequiresColumnList();

    // If CREATE TABLE does not have "AS query", a type must be specified for each column
    @BaseMessage("Type required for field ''{0}'' in CREATE TABLE without AS")
    ExInst<ValidatorException> createTableRequiresColumnTypes( String columnName );

    @BaseMessage("View ''{0}'' already exists and REPLACE not specified")
    ExInst<ValidatorException> viewExists( String name );

    @BaseMessage("Namespace ''{0}'' not found")
    ExInst<ValidatorException> schemaNotFound( String name );

    @BaseMessage("User ''{0}'' not found")
    ExInst<ValidatorException> userNotFound( String name );

    @BaseMessage("View ''{0}'' not found")
    ExInst<ValidatorException> viewNotFound( String name );

    @BaseMessage("Type ''{0}'' not found")
    ExInst<ValidatorException> typeNotFound( String name );

    @BaseMessage("Dialect does not support feature: ''{0}''")
    ExInst<ValidatorException> dialectDoesNotSupportFeature( String featureName );

    @BaseMessage("Substring error: negative substring length not allowed")
    ExInst<PolyphenyDbException> illegalNegativeSubstringLength();

    @BaseMessage("Trim error: trim character must be exactly 1 character")
    ExInst<PolyphenyDbException> trimError();

    @BaseMessage("Invalid types for arithmetic: {0} {1} {2}")
    ExInst<PolyphenyDbException> invalidTypesForArithmetic( String clazzName0, String op, String clazzName1 );

    @BaseMessage("Invalid types for comparison: {0} {1} {2}")
    ExInst<PolyphenyDbException> invalidTypesForComparison( String clazzName0, String op, String clazzName1 );

    @BaseMessage("Cannot convert {0} to {1}")
    ExInst<PolyphenyDbException> cannotConvert( String o, String toType );

    @BaseMessage("Invalid character for cast: {0}")
    ExInst<PolyphenyDbException> invalidCharacterForCast( String s );

    @BaseMessage("More than one value in list: {0}")
    ExInst<PolyphenyDbException> moreThanOneValueInList( String list );

    @BaseMessage("Failed to access field ''{0}'' of object of type {1}")
    ExInstWithCause<PolyphenyDbException> failedToAccessField( String fieldName, String typeName );

    @BaseMessage("Illegal jsonpath spec ''{0}'', format of the spec should be: ''<lax|strict> $'{'expr'}'''")
    ExInst<PolyphenyDbException> illegalJsonPathSpec( String pathSpec );

    @BaseMessage("Illegal jsonpath mode ''{0}''")
    ExInst<PolyphenyDbException> illegalJsonPathMode( String pathMode );

    @BaseMessage("Illegal jsonpath mode ''{0}'' in jsonpath spec: ''{1}''")
    ExInst<PolyphenyDbException> illegalJsonPathModeInPathSpec( String pathMode, String pathSpec );

    @BaseMessage("Strict jsonpath mode requires a non empty returned value, but is null")
    ExInst<PolyphenyDbException> strictPathModeRequiresNonEmptyValue();

    @BaseMessage("Illegal error behavior ''{0}'' specified in JSON_EXISTS function")
    ExInst<PolyphenyDbException> illegalErrorBehaviorInJsonExistsFunc( String errorBehavior );

    @BaseMessage("Empty result of JSON_VALUE function is not allowed")
    ExInst<PolyphenyDbException> emptyResultOfJsonValueFuncNotAllowed();

    @BaseMessage("Illegal empty behavior ''{0}'' specified in JSON_VALUE function")
    ExInst<PolyphenyDbException> illegalEmptyBehaviorInJsonValueFunc( String emptyBehavior );

    @BaseMessage("Illegal error behavior ''{0}'' specified in JSON_VALUE function")
    ExInst<PolyphenyDbException> illegalErrorBehaviorInJsonValueFunc( String errorBehavior );

    @BaseMessage("Strict jsonpath mode requires scalar value, and the actual value is: ''{0}''")
    ExInst<PolyphenyDbException> scalarValueRequiredInStrictModeOfJsonValueFunc( String value );

    @BaseMessage("Illegal wrapper behavior ''{0}'' specified in JSON_QUERY function")
    ExInst<PolyphenyDbException> illegalWrapperBehaviorInJsonQueryFunc( String wrapperBehavior );

    @BaseMessage("Empty result of JSON_QUERY function is not allowed")
    ExInst<PolyphenyDbException> emptyResultOfJsonQueryFuncNotAllowed();

    @BaseMessage("Illegal empty behavior ''{0}'' specified in JSON_VALUE function")
    ExInst<PolyphenyDbException> illegalEmptyBehaviorInJsonQueryFunc( String emptyBehavior );

    @BaseMessage("Strict jsonpath mode requires array or object value, and the actual value is: ''{0}''")
    ExInst<PolyphenyDbException> arrayOrObjectValueRequiredInStrictModeOfJsonQueryFunc( String value );

    @BaseMessage("Illegal error behavior ''{0}'' specified in JSON_VALUE function")
    ExInst<PolyphenyDbException> illegalErrorBehaviorInJsonQueryFunc( String errorBehavior );

    @BaseMessage("Null key of JSON object is not allowed")
    ExInst<PolyphenyDbException> nullKeyOfJsonObjectNotAllowed();

    @BaseMessage("Timeout of ''{0}'' ms for query execution is reached. Query execution started at ''{1}''")
    ExInst<PolyphenyDbException> queryExecutionTimeoutReached( String timeout, String queryStart );

    @BaseMessage("While executing SQL [{0}] on JDBC sub-namespace")
    ExInst<RuntimeException> exceptionWhilePerformingQueryOnJdbcSubSchema( String sql );

    @BaseMessage("There is no data store with this name: ''{0}''")
    ExInst<PolyphenyDbException> unknownStoreName( String store );

    @BaseMessage("Entity ''{0}'' is already placed on store ''{1}''")
    ExInst<PolyphenyDbException> placementAlreadyExists( String tableName, String storeName );

    @BaseMessage("There is no placement of entity ''{1}'' on store ''{0}''")
    ExInst<PolyphenyDbException> placementDoesNotExist( String storeName, String tableName );

    @BaseMessage("The field ''{0}'' is part of the primary key and cannot be dropped")
    ExInst<PolyphenyDbException> placementIsPrimaryKey( String name );

    @BaseMessage("There needs to be at least one placement per entity")
    ExInst<PolyphenyDbException> onlyOnePlacementLeft();

    @BaseMessage("The specified data store does not support the index method ''{0}''!")
    ExInst<PolyphenyDbException> unknownIndexMethod( String indexMethod );

    @BaseMessage("There is no placement of field ''{0}'' on the specified data store!")
    ExInst<PolyphenyDbException> missingColumnPlacement( String columnName );

    @BaseMessage("Unable to remove placement of field ''{0}'' because it is part of the index ''{1}''!")
    ExInst<PolyphenyDbException> indexPreventsRemovalOfPlacement( String indexName, String columnName );

    @BaseMessage("There is already an index with the name ''{0}''!")
    ExInst<PolyphenyDbException> indexExists( String indexName );

    @BaseMessage("The adapter name ''{0}'' refers to a data source. DDL statements are not allowed for data sources!")
    ExInst<PolyphenyDbException> ddlOnDataSource( String uniqueName );

    @BaseMessage("DDL statements are not allowed for tables of type source!")
    ExInst<PolyphenyDbException> ddlOnSourceTable();

    @BaseMessage("There is no adapter with this unique name: ''{0}''")
    ExInst<PolyphenyDbException> unknownAdapter( String store );

    @BaseMessage("There is no collation with this name: ''{0}''")
    ExInst<PolyphenyDbException> unknownCollation( String collationName );

    @BaseMessage("There is no query interface with this unique name: ''{0}''")
    ExInst<PolyphenyDbException> unknownQueryInterface( String name );

    @BaseMessage("There is no partition with this name: ''{0}''")
    ExInst<PolyphenyDbException> unknownPartitionType( String name );

    @BaseMessage("The partition names for a field need to be unique")
    ExInst<PolyphenyDbException> partitionNamesNotUnique();

}


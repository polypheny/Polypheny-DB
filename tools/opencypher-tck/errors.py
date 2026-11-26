"""Translate Polypheny Prism diagnostics to openCypher TCK error categories."""

import re


SYNTAX_DETAILS = {
    "AmbiguousAggregationExpression", "ColumnNameConflict", "CreatingVarLength",
    "DifferentColumnsInUnion", "FloatingPointOverflow", "IntegerOverflow", "InvalidAggregation",
    "InvalidArgumentPassingMode", "InvalidArgumentType", "InvalidClauseComposition", "InvalidDelete",
    "InvalidNumberLiteral", "InvalidNumberOfArguments", "InvalidParameterUse", "InvalidRelationshipPattern",
    "InvalidUnicodeCharacter", "InvalidUnicodeLiteral", "NegativeIntegerArgument", "NestedAggregation",
    "NoExpressionAlias", "NonConstantExpression", "NoSingleRelationshipType",
    "NoVariablesInScope", "RelationshipUniquenessViolation", "RequiresDirectedRelationship",
    "UndefinedVariable", "UnexpectedSyntax", "UnknownFunction", "VariableAlreadyBound", "VariableTypeConflict",
}
SYNTAX_MESSAGES = (
    (r"Cypher predicates require a BOOLEAN, got .+", "InvalidArgumentType"),
    (r"(?:keys|labels|properties|type)\(\) requires .+", "InvalidArgumentType"),
    (r"Relationship variable .+ is repeated in the same MATCH\.", "RelationshipUniquenessViolation"),
    (r"(?:The used variable is not known: |Unknown (?:sort expression: |variable with name )).+", "UndefinedVariable"),
    (r"\.{2}|-", "InvalidRelationshipPattern"),
    (r"Use exists\(pattern\) to evaluate a pattern as a value expression\.", "UnexpectedSyntax"),
    (r"Multiple result columns named '.+'\.", "ColumnNameConflict"),
    (r"RETURN \* requires a variable in scope\.", "NoVariablesInScope"),
)
ERROR_CLASS = re.compile(r"^(?:Error: |(?:[\w$]+\.)+(?:\w+Exception|AssertionError): )+")


def classify(error):
    explicit = tuple(error.get(field) for field in ("type", "phase", "detail"))
    if all(explicit):
        return explicit
    code = error.get("code")
    raw_message = error["message"].strip()
    wrappers = ERROR_CLASS.match(raw_message)
    message = raw_message[wrappers.end():] if wrappers else raw_message
    detail = message.partition(":")[0]
    if code in (None, "42601", "42000"):
        if detail == "MissingParameter" or re.fullmatch(r"Missing Cypher parameter: \$.+", message):
            return "ParameterMissing", "compile time", "MissingParameter"
        if detail == "Unknown Cypher procedure":
            return "ProcedureError", "compile time", "ProcedureNotFound"
        if detail in SYNTAX_DETAILS:
            return "SyntaxError", "compile time", detail
        for pattern, syntax_detail in SYNTAX_MESSAGES:
            if re.fullmatch(pattern, message):
                return "SyntaxError", "compile time", syntax_detail
        if code == "42601" and (wrappers and "org.polypheny.db.languages.NodeParseException: " in wrappers[0]
                                or re.match(r"(?:Encountered |Lexical error at line )", message)):
            return "SyntaxError", "compile time", "UnexpectedSyntax"
    if code == "42804" and detail == "InvalidArgumentType":
        return "TypeError", "compile time", detail
    if code == "2202E" and detail in {"InvalidArgumentType", "MapElementAccessByNonString"}:
        return "TypeError", "runtime", detail
    if code == "22003" and detail == "NumberOutOfRange":
        return "ArgumentError", "runtime", detail
    if code == "55000" and detail == "DeletedEntityAccess":
        return "EntityNotFound", "runtime", detail
    if code == "22023":
        if detail == "NegativeIntegerArgument" or re.fullmatch(
                r"InvalidArgumentType: (?:SKIP|LIMIT) requires a non-negative integer\.", message):
            return "SyntaxError", "runtime", detail
        if detail == "InvalidArgumentValue":
            return "TypeError", "runtime", detail
        if message == "InvalidArgumentType: range() requires integer arguments.":
            return "ArgumentError", "runtime", detail
    if code in (None, "XX000"):
        if message == "Used function is not supported!":
            return "SyntaxError", "compile time", "UnknownFunction"
        if message == "Cannot delete a node with relationships. Delete its relationships or use DETACH DELETE.":
            return "ConstraintVerificationFailed", "runtime", "DeleteConnectedNode"
        if detail == "MergeReadOwnWrites":
            return "SemanticError", "runtime", detail
        if detail == "InvalidPropertyType":
            return "TypeError", "runtime", detail
        if re.fullmatch(r"labels\(\) requires a node\.|type\(\) requires a relationship\.", message):
            return "TypeError", "runtime", "InvalidArgumentValue"
        if re.fullmatch(r"Unknown variable with name .+", message):
            return "SyntaxError", "compile time", "UndefinedVariable"
    return explicit

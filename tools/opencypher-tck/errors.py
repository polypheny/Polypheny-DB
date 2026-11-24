"""Translate Polypheny Prism diagnostics to openCypher TCK error categories."""

import re


SYNTAX_DETAILS = {
    "AmbiguousAggregationExpression", "CreatingVarLength", "InvalidAggregation",
    "InvalidArgumentType", "InvalidParameterUse", "InvalidRelationshipPattern",
    "NoSingleRelationshipType", "RelationshipUniquenessViolation", "UndefinedVariable",
    "UnexpectedSyntax", "VariableAlreadyBound", "VariableTypeConflict",
}
SYNTAX_MESSAGES = (
    (r"Cypher predicates require a BOOLEAN, got .+", "InvalidArgumentType"),
    (r"(?:keys|labels|properties|type)\(\) requires .+", "InvalidArgumentType"),
    (r"Relationship variable .+ is repeated in the same MATCH\.", "RelationshipUniquenessViolation"),
    (r"(?:The used variable is not known: |Unknown (?:sort expression: |variable with name )).+", "UndefinedVariable"),
    (r"\.{2}|-", "InvalidRelationshipPattern"),
    (r"Use exists\(pattern\) to evaluate a pattern as a value expression\.", "UnexpectedSyntax"),
)
ERROR_CLASS = re.compile(r"^(?:Error: |(?:[\w$]+\.)+(?:\w+Exception|AssertionError): )+")


def classify(error):
    explicit = tuple(error.get(field) for field in ("type", "phase", "detail"))
    if all(explicit):
        return explicit
    code = error.get("code")
    message = ERROR_CLASS.sub("", error["message"]).strip()
    detail = message.partition(":")[0]
    if code in (None, "42601", "42000"):
        if detail in SYNTAX_DETAILS:
            return "SyntaxError", "compile time", detail
        for pattern, syntax_detail in SYNTAX_MESSAGES:
            if re.fullmatch(pattern, message):
                return "SyntaxError", "compile time", syntax_detail
    if code == "42804" and detail == "InvalidArgumentType":
        return "TypeError", "compile time", detail
    if code in (None, "XX000"):
        if detail == "MergeReadOwnWrites":
            return "SemanticError", "runtime", detail
        if detail == "InvalidPropertyType":
            return "TypeError", "runtime", detail
        if re.fullmatch(r"labels\(\) requires a node\.|type\(\) requires a relationship\.", message):
            return "TypeError", "runtime", "InvalidArgumentValue"
        if re.fullmatch(r"Unknown variable with name .+", message):
            return "SyntaxError", "compile time", "UndefinedVariable"
    return explicit

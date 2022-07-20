package org.polypheny.db.languages.polyscript;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class Expression {
    private final String value;


    private final List<String> namedArguments;
    private final static Pattern NAMED_ARGUMENTS_PATTERN = Pattern.compile(":\\w+");

    public Expression(String value) {
        this.value = value;
        this.namedArguments = parseNamedArguments();
    }

    public Expression parameterize(Map<String, Object> arguments) {
        String newValue = this.value;
        for(String parameter : namedArguments) {
            newValue = replaceParameter(arguments, newValue, parameter);
        }
        return newInstance(newValue);
    }

    abstract Expression newInstance(String script);

    private String replaceParameter(Map<String, Object> arguments, String newValue, String parameter) {
        Object argumentValue = arguments.get(parameter);
        if(argumentValue != null) {
            newValue = newValue.replace(addColonPrefix(parameter), stringify(argumentValue));
        }
        return newValue;
    }

    // TODO(nic): Maybe override in subclasses to handle language-specific string formats
    private String stringify(Object argumentValue) {
//        if(argumentValue instanceof String) {
//            return wrapWithSingleQuotes(argumentValue);
//        }
        return argumentValue.toString();
    }

    private String wrapWithSingleQuotes(Object argumentValue) {
        return "'" + argumentValue + "'";
    }

    private String addColonPrefix(String parameter) {
        return ":" + parameter;
    }

    private List<String> parseNamedArguments() {
        List<String> namedArguments = new ArrayList<>();
        final Matcher matcher = NAMED_ARGUMENTS_PATTERN.matcher(this.value);
        while (matcher.find()) {
            String argument = matcher.group(0);
            namedArguments.add(argument.substring(1)); // strip prefix ':'
        }
        return namedArguments;
    }

    public String getValue() {
        return value;
    }

    public List<String> getNamedArguments() {
        return Collections.unmodifiableList(namedArguments);
    }

    @Override
    public String toString() {
        return getClass().getName() + ": { expression: " + value + "}";
    }
}

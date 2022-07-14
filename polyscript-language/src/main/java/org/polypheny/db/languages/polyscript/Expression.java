package org.polypheny.db.languages.polyscript;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Expression {
    private final String value;


    private final List<String> namedArguments;
    private final static Pattern NAMED_ARGUMENTS_PATTERN = Pattern.compile(":\\w+");

    public Expression(String value) {
        this.value = value;
        this.namedArguments = parseNamedArguments();
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

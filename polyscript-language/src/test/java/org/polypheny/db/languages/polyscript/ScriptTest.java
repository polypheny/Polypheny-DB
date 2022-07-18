package org.polypheny.db.languages.polyscript;

import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertNotEquals;

public class ScriptTest {

    @Test
    public void testParameterize() {
        String query = "sql(insert into students VALUES(:id, :name);";
        Map<String, Object> arguments = Map.of("id", 1, "name", "James");
        Expression expression = new SqlExpression(query);
        Script sut = new Script(List.of(expression));

        Script parameterizedScript = sut.parameterize(arguments);

        assertNotEquals(parameterizedScript, sut);
        Expression firstParameterized = getFirst(parameterizedScript);
        Expression firstNamed = getFirst(sut);
        assertNotEquals(firstNamed, firstParameterized);
    }

    private Expression getFirst(Script parameterizedScript) {
        return parameterizedScript.stream().collect(Collectors.toList()).get(0);
    }
}

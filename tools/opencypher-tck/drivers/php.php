<?php

declare(strict_types=1);

use Polypheny\Connection;
use Polypheny\Decimal;
use Polypheny\Edge;
use Polypheny\Graph;
use Polypheny\Node;
use Polypheny\Path;
use Polypheny\ServerException;

function encodeMap(array|stdClass $values): stdClass
{
    $result = new stdClass();
    foreach ($values as $key => $value) {
        $result->{(string) $key} = encodeValue($value);
    }
    return $result;
}

function encodeValue(mixed $value): mixed
{
    if ($value instanceof Node || $value instanceof Edge) {
        $element = [
            '$type' => $value instanceof Node ? 'node' : 'edge',
            'id' => $value->id,
            'labels' => $value->labels,
            'properties' => encodeMap($value->properties),
        ];
        if ($value instanceof Edge) {
            $element['left'] = $value->left;
            $element['right'] = $value->right;
        }
        return $element;
    }
    if ($value instanceof Path || $value instanceof Graph) {
        return [
            '$type' => $value instanceof Path ? 'path' : 'graph',
            'nodes' => encodeValue($value->nodes),
            'edges' => encodeValue($value->edges),
        ];
    }
    if ($value instanceof stdClass) {
        return ['$type' => 'map', 'entries' => encodeMap($value)];
    }
    if (is_array($value)) {
        return array_map('encodeValue', $value);
    }
    if ($value instanceof Decimal) {
        return ['$type' => 'float', 'value' => (string) $value];
    }
    if (is_float($value) && !is_finite($value)) {
        return ['$type' => 'float', 'value' => is_nan($value) ? 'NaN' : ($value > 0 ? 'Infinity' : '-Infinity')];
    }
    if ($value === null || is_scalar($value)) {
        return $value;
    }
    throw new RuntimeException('Unsupported result value ' . get_debug_type($value));
}

function executeRequest(Connection $connection, stdClass $request): array|stdClass
{
    if ($request->op === 'commit') {
        $connection->commit();
        return new stdClass();
    }
    if ($request->op === 'rollback') {
        $connection->rollback();
        return new stdClass();
    }
    if ($request->op !== 'query') {
        throw new RuntimeException('Unknown operation ' . $request->op);
    }

    $cursor = $connection->cursor();
    try {
        $cursor->executeAny($request->language, $request->statement, $request->params, namespace: $request->namespace);
        $columns = array_column($cursor->description() ?? [], 'name');
        $rows = $cursor->rowCount() >= 0 ? [] : $cursor->fetchAll();
        return ['columns' => $columns, 'rows' => encodeValue($rows)];
    } finally {
        $cursor->close();
    }
}

try {
    $autoload = getenv('M23_PHP_AUTOLOAD');
    if ($autoload === false || !is_file($autoload)) {
        throw new RuntimeException('Set M23_PHP_AUTOLOAD to the driver checkout’s vendor/autoload.php');
    }
    require $autoload;
    $connection = Connection::connectTcp(
        getenv('M23_HOST'),
        (int) getenv('M23_PORT'),
        getenv('M23_USERNAME'),
        getenv('M23_PASSWORD'),
    );
    try {
        $connection->setClientInfoProperties(['CypherResultFormat' => 'relational']);
        while (($line = fgets(STDIN)) !== false) {
            try {
                $request = json_decode($line, flags: JSON_THROW_ON_ERROR);
                $response = executeRequest($connection, $request);
            } catch (Throwable $error) {
                $details = ['message' => $error->getMessage()];
                if ($error instanceof ServerException && $error->getSqlState() !== null) {
                    $details['code'] = $error->getSqlState();
                }
                $response = ['error' => $details];
            }
            echo json_encode($response, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION), "\n";
        }
    } finally {
        $connection->close();
    }
} catch (Throwable $error) {
    fwrite(STDERR, $error->getMessage() . "\n");
    exit(1);
}

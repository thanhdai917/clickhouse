<?php
namespace Sk3\Clickhouse;

interface Connector {
    function __construct(string $connectionString);

    /**
     * @param string $connectionString
     *
     * @return $this
     */
    public function connect(string $connectionString): Connector;

    /**
     * @param string $query
     * @param int $itemsPerPage
     * @return array
     */
    public function paginate(string $query, int $itemsPerPage): array;
}

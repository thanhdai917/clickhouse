<?php
namespace Sk3\Clickhouse;

use Sk3\Clickhouse\Util\CSVParsers;

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
     * @param int $page
     * @return array
     */
    public function paginate(string $query, int $itemsPerPage, int $page): array;

    /**
     * @param string $tableName
     * @param array $columns
     * @return bool
     */
    public function createTable(string $tableName, array $columns): bool;

    /**
     * @param string $sql
     * @return bool
    */
    public function write(string $sql): bool;

    /**
     * @param           $table
     * @param CSVParsers $parser
     * @param array $columns
     * @return int total rows import
     */
    public function import($table, CSVParsers $parser, array $columns): int;

    /**
     * @param string $table
     * @return array
     */
    public function getTableDetail(string $table): array;

    /**
     * @param string $query
     * @return int
     * @throws ClickHouseConnectorException
     * @throws DBConnectorException
     */

    public function getTotalItems(string $query): int;
}

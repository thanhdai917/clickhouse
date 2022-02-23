<?php

use ClickHouseDB\Exception\DatabaseException;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Log;
use Sk3\Clickhouse\Column;
use Sk3\Clickhouse\DBConnectorException;
use Sk3\Clickhouse\Manager\ClickhouseConnector;
use Sk3\Clickhouse\SelectResult;

class ClickHouseSelectResult implements SelectResult {

    const SPECIAL_CHARS = [
        '\N' => NULL,
    ];
    /**
     * @var resource stream resource
     */
    private $responseStream;

    /**
     * @var string
     */
    private $query;

    /**
     * @var array;
     */
    private $columns = [];

    function __construct($responseStream, $query, array $bindParams) {
        $fields = fgetcsv($responseStream, 9999, "\t");
        $types = fgetcsv($responseStream, 9999, "\t");
        foreach ($fields as $index => $field) {
            $this->columns[] = [
                'name' => stripcslashes($field),
                'type' => stripcslashes($types[$index]),
            ];
        }
        $this->responseStream = $responseStream;
        $this->query = $query;
        $this->bindParams = $bindParams;
    }

    /**
     * @return Collection Collection of column
     * @throws DBConnectorException
     */
    public function getColumns(): Collection {
        try {
            $columns = collect();
            foreach ($this->columns as $column) {
                $columns->push(
                    new Column(
                        $column['name'],
                        ClickHouseConnector::convertToPHPType($column['type'])
                    )
                );
            }
            return $columns;
        } catch (DatabaseException $exception) {
            Log::error($exception);
            throw new DBConnectorException($exception->getMessage());
        }
    }

    /**
     * @param int|null $limit
     *
     * @return Collection Collection of row
     */

    public function fetchAll(int $limit = NULL): Collection {
        $rows = $this->getRows();

        $out = [];
        foreach ($rows as $row) {
            $out = $row;
            break;
        }

        return $out;
    }

    /**
     * @return array Collection of row
     */
    public function fetchOne(): array {
        $rows = $this->getRows();
        $out = [];
        foreach ($rows as $row) {
            $out = $row;
            break;
        }

        return $out;
    }

    public function getRows(int $limit = NULL): Collection {
        if ($limit === NULL) {
            $limit = 9999;
        }
        $position = 1;
        $rows = collect();
        $specialCharacters = array_keys(self::SPECIAL_CHARS);
        while ($position <= $limit && ($rowData = fgetcsv($this->responseStream, 0, "\t"))) {
            $position++;
            $row = [];
            foreach ($rowData as $colIndex => $colData) {
                $row[$this->columns[$colIndex]['name']] = (
                in_array($colData, $specialCharacters) ? self::SPECIAL_CHARS[$colData] : stripcslashes($colData)
                );
            }
            $rows->push($row);
        }
        return $rows;
    }
}
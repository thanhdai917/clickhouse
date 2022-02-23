<?php


namespace Sk3\Clickhouse;
use Illuminate\Support\Collection;

interface SelectResult {
    /**
     * @return Collection Collection of column
     */
    public function getColumns(): Collection;

    /**
     * @param int|null $limit
     *
     * @return Collection Collection of row
     */

    public function fetchAll(int $limit = NULL): Collection;

    /**
     * @return array of row
     */
    public function fetchOne(): array;
}

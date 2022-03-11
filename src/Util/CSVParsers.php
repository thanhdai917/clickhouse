<?php

namespace Sk3\Clickhouse\Util;

class CSVParsers {
    /**
     * @var string
     */
    private mixed $separator;

    /**
     * @var string
     */
    private mixed $hasHeader;

    /**
     * @var string
     */
    private mixed $enclosure;

    /**
     * @var resource
     */
    private $stream;

    /**
     * @var string[]
     */
    private array|false $fields;

    /**
     * CSVParser constructor.
     *
     * @param resource $file stream resource
     */
    public function __construct($file, $separator = NULL, $enclosure = NULL, $hasHeader = TRUE) {
        $stream = $file;
        if (!$enclosure) {
            $enclosure = '"';
        }
        if (!$separator) {
            $firstLine = fgets($stream);
            $separator = $this->detectSeparator($firstLine);
            rewind($stream);
        }
        $this->enclosure = $enclosure;
        $this->separator = $separator;
        $this->hasHeader = $hasHeader;
        $this->stream = $stream;

        $fields = fgetcsv($this->stream, 0, $this->separator, $this->enclosure);
        $this->fields = $fields;
        $this->resetPointer();
    }

    public function resetPointer() {
        rewind($this->stream);
        if ($this->hasHeader) {
            fgets($this->stream);
        }
    }

    /**
     * @return array|false
     * @throws Exception
     */
    public function fetchRow($columnsIndex = NULL): bool|array {
        $row = fgetcsv(
            $this->stream,
            0,
            $this->separator,
            $this->enclosure
        );
        if (!$columnsIndex) {
            return $row;
        }
        $filteredRow = [];
        foreach ($columnsIndex as $index) {
            if (isset($row[$index])) {
                $filteredRow[] = $row[$index];
            } else {
                throw new \Exception(
                    "Wrong data format: " . PHP_EOL
                    . json_encode($columnsIndex) . PHP_EOL
                    . json_encode($row) . PHP_EOL
                );
            }
        }
        return $filteredRow;
    }

    /**
     * @param int $chunkSize
     *
     * @return array
     * @throws Exception
     */
    public function fetchRows(int $chunkSize = 1000, $columnsIndex = NULL): array {
        $rows = [];
        $count = 0;
        while (
            $count < $chunkSize
            && ($row = fgetcsv($this->stream, 0, $this->separator, $this->enclosure))
        ) {
            if (count($row) == 1 && !trim($row[0])) {
                continue;
            }
            if (!$columnsIndex) {
                $rows[] = $row;
                continue;
            }
            $filteredRow = [];
            foreach ($columnsIndex as $key => $index) {
                if (isset($row[$index])) {
                    $filteredRow[] = $row[$index];
                } else {
                    throw new \Exception(
                        "Wrong data format: " . PHP_EOL
                        . json_encode($columnsIndex) . PHP_EOL
                        . json_encode($row) . PHP_EOL
                    );
                }
            }
            $rows[] = $filteredRow;
            $count++;
        }
        return $rows;
    }

    /**
     * @param string $firstLine
     *
     * @return string
     */
    public static function detectSeparator(string $firstLine): string {
        $delimiters = [",", ";", "\t", "|"];
        $detectedDelimiter = ",";
        $maxCount = 0;
        foreach ($delimiters as $delimiter) {
            if (
                str_contains($firstLine, $delimiter)
                && ($count = count(explode($delimiter, $firstLine))) > $detectedDelimiter
            ) {
                $maxCount = $count;
                $detectedDelimiter = $delimiter;
            }
        }
        return $detectedDelimiter;
    }

    public function getFields(): array|bool {
        return $this->fields;
    }

    public function getEnclosure() {
        return $this->enclosure;
    }

    public function getSeparator() {
        return $this->separator;
    }

    public function checkHasHeader() {
        return $this->hasHeader;
    }
}

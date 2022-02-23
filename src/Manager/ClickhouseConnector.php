<?php
namespace Sk3\Clickhouse\Manager;

use Sk3\Clickhouse\Column;
use Sk3\Clickhouse\Connector;
use Sk3\Clickhouse\Util\EmulateBindParam;
use GuzzleHttp\Client;
use GuzzleHttp\Exception\BadResponseException;
use GuzzleHttp\Exception\GuzzleException;

class ClickhouseConnector implements Connector {
    use EmulateBindParam;
    const DEFAULT_PORT = 8123;
    const DEFAULT_HOST = 'localhost';
    const TYPE_MAP = [
        'UInt8'       => Column::TYPE_NUMBER,
        'UInt16'      => Column::TYPE_NUMBER,
        'UInt32'      => Column::TYPE_NUMBER,
        'UInt64'      => Column::TYPE_NUMBER,
        'UInt256'     => Column::TYPE_NUMBER,
        'Int8'        => Column::TYPE_NUMBER,
        'Int16'       => Column::TYPE_NUMBER,
        'Int32'       => Column::TYPE_NUMBER,
        'Int64'       => Column::TYPE_NUMBER,
        'Int128'      => Column::TYPE_NUMBER,
        'Int256'      => Column::TYPE_NUMBER,
        'Float32'     => Column::TYPE_NUMBER,
        'Float64'     => Column::TYPE_NUMBER,
        'Decimal'     => Column::TYPE_NUMBER,
        'Decimal32'   => Column::TYPE_NUMBER,
        'Decimal64'   => Column::TYPE_NUMBER,
        'Decimal128'  => Column::TYPE_NUMBER,
        'Decimal256'  => Column::TYPE_NUMBER,
        'String'      => Column::TYPE_STRING,
        'Fixedstring' => Column::TYPE_STRING,
        'Date'        => Column::TYPE_DATE_TIME,
        'Datetime'    => Column::TYPE_DATE_TIME,
        'Datetime64'  => Column::TYPE_DATE_TIME,
    ];
    private $config;

    public function __construct(string $connectionString) {

    }

    public function connect(string $connectionString): Connector {
        $config = json_decode($connectionString, true);
        if($config === null) {
            throw new ClickHouseConnectorException('Wrong format json');
        }
        try {
            if (empty($config['port'])) {
                $config['port'] = (string) self::DEFAULT_PORT;
            }
            if (empty($config['host'])) {
                $config['host'] = (string) self::DEFAULT_HOST;
            }
            $this->config = $config;
        } catch (\Exception $e) {
            throw new ClickHouseConnectorException(
                $e->getMessage(),
                $e->getCode(),
                $e->getPrevious()
            );
        }
    }

    private function sendQuery($query, $params = [], $format = 'TabSeparatedWithNamesAndTypes') {
        $query = trim($query, " \t\n\r\0\x0B;");
        $query = self::emulateBindParam($query, $params);
        $host = urlencode($this->config['host']);
        $port = urlencode($this->config['port']);
        $database = urlencode($this->config['database']);

        $client = new Client([
            'timeout' => 299000999000,
            'headers' => [
                'X-ClickHouse-User' => $this->config['username'],
                'X-ClickHouse-Key'  => $this->config['password'],
                'X-ClickHouse-Format'  => $format ? $format : 'TabSeparated',
                'Content-Type' => 'text/plain'
            ]
        ]);

        $protocol = 'http';
        if (!empty($this->config['protocol'])) {
            $protocol = 'https';
        }

        try {
            $response = $client->post("$protocol://{$host}:{$port}/?database={$database}", [
                'body' => $query,
            ]);
        } catch (BadResponseException $e) {
            throw new ClickHouseConnectorException((string) $e->getResponse()->getBody());
        } catch (GuzzleException $e) {
            throw new ClickHouseConnectorException($e->getMessage());
        }
        return $response->getBody()->detach();
    }

    static function convertToPHPType(string $nativeType): string {
        if (strpos($nativeType, 'Date') === 0) {
            return Column::TYPE_DATE_TIME;
        }
        if (!empty(self::TYPE_MAP[$nativeType])) {
            return self::TYPE_MAP[$nativeType];
        }
        return Column::TYPE_OTHERS;
    }
}
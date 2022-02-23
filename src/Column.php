<?php
namespace Sk3\Clickhouse;

class Column {
    const TYPE_NUMBER = 'number';
    const TYPE_BOOLEAN = 'boolean';
    const TYPE_STRING = 'string';
    const TYPE_DATE_TIME = 'datetime';
    const TYPE_OTHERS = 'other';

    protected $name;
    protected $type;

    /**
     * Column constructor.
     *
     * @param string $name using Column::TYPE_*
     * @param string $type
     */
    public function __construct(string $name, string $type) {
        $this->name = $name;
        $this->type = $type;
    }

    public function getName(): string {
        return $this->name;
    }

    public function getType(): string {
        return $this->type;
    }

    public function toArray(): array {
        return [
            'name' => $this->name,
            'type' => $this->type,
        ];
    }
}

<?php

if (!function_exists('parseBindParamValue')) {
    function parseBindParamValue(string $value): array {
        $paramTypePattern = '/^(?:(String|Int|Float|Date|Datetime|Boolean|Expression)(\[\])?:)?(.+)$/';
        preg_match($paramTypePattern, $value, $matches);
        if (preg_match($paramTypePattern, $value, $matches)) {
            return [
                // type => String|Int|Float|Date|Datetime|Boolean|Expression
                $matches[1] ? $matches[1] : 'String',
                // value => string
                $matches[3],
                // is an array (true => yes, false => no)
                (bool) $matches[2],
            ];
        } else {
            return [
                'String',
                '',
                FALSE,
            ];
        }
    }
}
<?php
declare(strict_types = 1);

namespace Simbiat\Database;

use Simbiat\CuteBytes;
use Simbiat\SandClock;

use function is_array, is_string;

/**
 * Functions to handle bindings
 */
final class Bind
{
    /**
     * Function mainly for convenience and some types enforcing, which sometimes 'fail' in PDO itself
     * @param \PDOStatement $sql      Query to process.
     * @param array         $bindings List of bindings as an array. Each item should be either just a non-array value or an array in format `[$value, 'type_of_the_value']`.
     *
     * @return \PDOStatement
     */
    public static function binding(\PDOStatement $sql, array $bindings = []): \PDOStatement
    {
        try {
            foreach ($bindings as $binding => $value) {
                #Skip the binding if it's not present in the query.
                if (!str_contains($sql->queryString, $binding)) {
                    continue;
                }
                if (!is_array($value)) {
                    #Handle malformed UTF for strings
                    if (is_string($value)) {
                        $value = mb_scrub($value, 'UTF-8');
                    }
                    $sql->bindValue($binding, $value);
                    continue;
                }
                #Handle malformed UTF for strings
                if (is_string($value[0])) {
                    $value[0] = mb_scrub($value[0], 'UTF-8');
                }
                if (!isset($value[1]) || !is_string($value[1])) {
                    $value[1] = '';
                }
                switch (mb_strtolower($value[1], 'UTF-8')) {
                    case 'date':
                        if (method_exists(SandClock::class, 'format')) {
                            $sql->bindValue($binding, SandClock::format($value[0], 'Y-m-d'));
                        } else {
                            $sql->bindValue($binding, (string)$value[0]);
                        }
                        break;
                    case 'time':
                        if (method_exists(SandClock::class, 'format')) {
                            $sql->bindValue($binding, SandClock::format($value[0], 'H:i:s.u'));
                        } else {
                            $sql->bindValue($binding, (string)$value[0]);
                        }
                        break;
                    case 'datetime':
                        if (method_exists(SandClock::class, 'format')) {
                            $sql->bindValue($binding, SandClock::format($value[0]));
                        } else {
                            $sql->bindValue($binding, (string)$value[0]);
                        }
                        break;
                    case 'bool':
                    case 'boolean':
                        $sql->bindValue($binding, (bool)$value[0], \PDO::PARAM_BOOL);
                        break;
                    case 'null':
                        $sql->bindValue($binding, null, \PDO::PARAM_NULL);
                        break;
                    case 'int':
                    case 'integer':
                    case 'number':
                    case 'limit':
                    case 'offset':
                        $sql->bindValue($binding, (int)$value[0], \PDO::PARAM_INT);
                        break;
                    case 'str':
                    case 'string':
                    case 'text':
                    case 'float':
                    case 'varchar':
                    case 'varchar2':
                        $sql->bindValue($binding, (string)$value[0]);
                        break;
                    case 'bytes':
                    case 'bits':
                        if (method_exists(CuteBytes::class, 'bytes')) {
                            $sql->bindValue($binding, CuteBytes::bytes((string)$value[0], 1024, bits: mb_strtolower($value[1], 'UTF-8') === 'bits'));
                        } else {
                            $sql->bindValue($binding, (string)$value[0]);
                        }
                        break;
                    case 'match':
                        #Same as string, but for MATCH operator, when your string can have special characters, that will break the query
                        $sql->bindValue($binding, self::match((string)$value[0]));
                        break;
                    case 'like':
                        #Same as string, but wrapped in % for LIKE '%string%'
                        $sql->bindValue($binding, '%'.$value[0].'%');
                        break;
                    case 'lob':
                    case 'large':
                    case 'object':
                    case 'blob':
                        #Suppress warning from custom inspection, since we are dealing with binary data here, so use of mb_strlen is not appropriate
                        /** @noinspection NoMBMultibyteAlternative */
                        $sql->bindParam($binding, $value[0], \PDO::PARAM_LOB, \strlen($value[0]));
                        break;
                    default:
                        if (\is_int($value[1])) {
                            $sql->bindValue($binding, $value[0], $value[1]);
                        } else {
                            $sql->bindValue($binding, (string)$value[0]);
                        }
                }
            }
        } catch (\Throwable $exception) {
            $errMessage = 'Failed to bind variable `'.$binding.'`';
            if (is_array($value)) {
                $errMessage .= ' of type `'.$value[1].'` with value `'.$value[0].'`';
            } else {
                $errMessage .= ' with value `'.$value.'`';
            }
            throw new \RuntimeException($errMessage, 0, $exception);
        }
        return $sql;
    }
    
    /** Helper function to prepare string binding for MATCH in FULLTEXT search
     * @param string $string
     *
     * @return string
     */
    public static function match(string $string): string
    {
        $newValue = preg_replace([
            #Trim first
            '/^[\p{Z}\h\v\r\n]+|[\p{Z}\h\v\r\n]+$/u',
            #Remove all symbols except allowed operators and space. @distance is not included, since it's unlikely a human will be using it through a UI form
            '/[^\p{L}\p{N}_+\-<>~()"* ]/u',
            #Remove all operators that can only precede a text and that are not preceded by either beginning of string or space
            '/(?<!^| )[+\-<>~]/u',
            #Remove all double quotes and asterisks that are not preceded by either beginning of string, letter, number or space
            '/(?<![\p{L}\p{N}_ ]|^)[*"]/u',
            #Remove all double quotes and asterisks that are inside a text
            '/([\p{L}\p{N}_])([*"])([\p{L}\p{N}_])/u',
            #Remove all opening parentheses, which are not preceded by the beginning of string or space
            '/(?<!^| )\(/u',
            #Remove all closing parentheses, which are not preceded by the beginning of string or space or are not followed by the end of string or space
            '/(?<![\p{L}\p{N}_])\)|\)(?! |$)/u'
        ], '', $string);
        #Remove all double quotes if the count is not even
        if (mb_substr_count($newValue, '"', 'UTF-8') % 2 !== 0) {
            $newValue = preg_replace('/"/u', '', $newValue);
        }
        #Remove all parentheses if the count of closing does not match the count of opening ones
        if (mb_substr_count($newValue, '(', 'UTF-8') !== mb_substr_count($newValue, ')', 'UTF-8')) {
            $newValue = preg_replace('/[()]/u', '', $newValue);
        }
        $newValue = preg_replace([
            #Remove all operators that can only precede a text and that do not have a text after them (at the end of a string). Do this for any possible combinations
            '/[+\-<>~]+$/u',
            #Remove the asterisk operator at the beginning of a string
            '/^\*/u'
        ], '', $newValue);
        #Check if the new value is just the set of operators and if it is - set the value to an empty string
        if (preg_match('/^[+\-<>~()"*]+$/u', $newValue)) {
            $newValue = '';
        }
        return $newValue;
    }
    
    /**
     * Function to unpack IN bindings
     *
     * @param string $sql      Query to process
     * @param array  $bindings List of bindings
     *
     * @return void
     */
    public static function unpackIN(string &$sql, array &$bindings): void
    {
        #First unpack IN binding
        $allInBindings = [];
        foreach ($bindings as $binding => $value) {
            if (is_array($value) && mb_strtolower($value[1], 'UTF-8') === 'in') {
                if (!is_array($value[0])) {
                    throw new \UnexpectedValueException('When using `in` binding only array is allowed');
                }
                #Check if a type is set
                if (empty($value[2]) || !is_string($value[2])) {
                    $value[2] = 'string';
                }
                #Prevent attempts on IN recursion
                if ($value[2] === 'in') {
                    throw new \UnexpectedValueException('Can\'t use `in` type when already using `in` binding');
                }
                #Ensure we have a non-associative array
                $value[0] = array_values($value[0]);
                $inBindings = [];
                #Generate list of
                foreach ($value[0] as $inCount => $inItem) {
                    $inBindings[$binding.'_'.$inCount] = [$inItem, $value[2]];
                    $allInBindings[$binding.'_'.$inCount] = [$inItem, $value[2]];
                }
                unset($bindings[$binding]);
                #Update the query
                $sql = str_replace($binding, implode(', ', array_keys($inBindings)), $sql);
            }
        }
        $bindings = array_merge($bindings, $allInBindings);
    }
}
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
     * Array to link various data types to their respective binding handlers. Key represents a data type, value is the handler function name.
     * @var array|string[]
     */
    private static array $bindingHandlers = [
        'date' => 'bindDate',
        'time' => 'bindTime',
        'datetime' => 'bindDateTime',
        'bool' => 'bindBoolean',
        'boolean' => 'bindBoolean',
        'null' => 'bindNull',
        'int' => 'bindInteger',
        'integer' => 'bindInteger',
        'number' => 'bindInteger',
        'limit' => 'bindInteger',
        'offset' => 'bindInteger',
        'str' => 'bindString',
        'string' => 'bindString',
        'text' => 'bindString',
        'float' => 'bindString',
        'varchar' => 'bindString',
        'varchar2' => 'bindString',
        'bytes' => 'bindBytes',
        'bits' => 'bindBits',
        'match' => 'bindMatch',
        'like' => 'bindLike',
        'lob' => 'bindBinary',
        'large' => 'bindBinary',
        'object' => 'bindBinary',
        'blob' => 'bindBinary',
    ];
    
    /**
     * Function mainly for convenience and some types enforcing, which sometimes 'fail' in PDO itself
     *
     * @param \PDOStatement $sql      Query to process.
     * @param array         $bindings List of bindings as an array. Each item should be either just a non-array value or an array in format `[$value, 'type_of_the_value']`.
     *
     * @return void
     */
    public static function binding(\PDOStatement $sql, array $bindings = []): void
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
                $type = mb_strtolower($value[1], 'UTF-8');
                $handler = self::$bindingHandlers[$type] ?? null;
                if ($handler && method_exists(self::class, $handler)) {
                    self::$handler($sql, $binding, $value[0]);
                } elseif (\is_int($value[1])) {
                    $sql->bindValue($binding, $value[0], $value[1]);
                } else {
                    $sql->bindValue($binding, (string)$value[0]);
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
    }
    
    /**
     * Bind value to parameter identifier in PDOStatement's query as date. If `\Simbiat\SandClock` is available will try to format as `Y-m-d`, otherwise expects a properly formatted string.
     * @param \PDOStatement $sql     PDOStatement to use
     * @param string        $binding Identifier name
     * @param mixed         $value   Value to bind
     *
     * @return void
     */
    public static function bindDate(\PDOStatement $sql, string $binding, mixed $value): void
    {
        if (method_exists(SandClock::class, 'format')) {
            self::bindString($sql, $binding, SandClock::format($value, 'Y-m-d'));
        } else {
            self::bindString($sql, $binding, (string)$value);
        }
    }
    
    /**
     * Bind value to parameter identifier in PDOStatement's query as time. If `\Simbiat\SandClock` is available will try to format as `H:i:s.u`, otherwise expects a properly formatted string.
     * @param \PDOStatement $sql     PDOStatement to use
     * @param string        $binding Identifier name
     * @param mixed         $value   Value to bind
     *
     * @return void
     */
    public static function bindTime(\PDOStatement $sql, string $binding, mixed $value): void
    {
        if (method_exists(SandClock::class, 'format')) {
            self::bindString($sql, $binding, SandClock::format($value, 'H:i:s.u'));
        } else {
            self::bindString($sql, $binding, (string)$value);
        }
    }
    
    /**
     * Bind value to parameter identifier in PDOStatement's query as datetime. If `\Simbiat\SandClock` is available will try to format as `Y-m-d H:i:s.u`, otherwise expects a properly formatted string.
     * @param \PDOStatement $sql     PDOStatement to use
     * @param string        $binding Identifier name
     * @param mixed         $value   Value to bind
     *
     * @return void
     */
    public static function bindDateTime(\PDOStatement $sql, string $binding, mixed $value): void
    {
        if (method_exists(SandClock::class, 'format')) {
            self::bindString($sql, $binding, SandClock::format($value));
        } else {
            self::bindString($sql, $binding, (string)$value);
        }
    }
    
    /**
     * Bind value to parameter identifier in PDOStatement's query as boolean.
     * @param \PDOStatement $sql     PDOStatement to use
     * @param string        $binding Identifier name
     * @param mixed         $value   Value to bind
     *
     * @return void
     */
    public static function bindBoolean(\PDOStatement $sql, string $binding, mixed $value): void
    {
        $sql->bindValue($binding, (bool)$value, \PDO::PARAM_BOOL);
    }
    
    /**
     * Bind value to parameter identifier in PDOStatement's query as null.
     *
     * @param \PDOStatement $sql     PDOStatement to use
     * @param string        $binding Identifier name
     * @param mixed         $value   Value to bind
     *
     * @return void
     * @noinspection PhpUnusedParameterInspection
     * */
    public static function bindNull(\PDOStatement $sql, string $binding, mixed $value): void
    {
        $sql->bindValue($binding, null, \PDO::PARAM_NULL);
    }
    
    /**
     * Bind value to parameter identifier in PDOStatement's query as integer.
     * @param \PDOStatement $sql     PDOStatement to use
     * @param string        $binding Identifier name
     * @param mixed         $value   Value to bind
     *
     * @return void
     */
    public static function bindInteger(\PDOStatement $sql, string $binding, mixed $value): void
    {
        $sql->bindValue($binding, (int)$value, \PDO::PARAM_INT);
    }
    
    /**
     * Bind value to parameter identifier in PDOStatement's query as string.
     * @param \PDOStatement $sql     PDOStatement to use
     * @param string        $binding Identifier name
     * @param mixed         $value   Value to bind
     *
     * @return void
     */
    public static function bindString(\PDOStatement $sql, string $binding, mixed $value): void
    {
        $sql->bindValue($binding, (string)$value);
    }
    
    /**
     * Bind value to parameter identifier in PDOStatement's query as bytes value (string). If `\Simbiat\CuteBytes` is not available, the value will be bound as is.
     * @param \PDOStatement $sql     PDOStatement to use
     * @param string        $binding Identifier name
     * @param mixed         $value   Value to bind
     *
     * @return void
     */
    public static function bindBytes(\PDOStatement $sql, string $binding, mixed $value): void
    {
        if (method_exists(CuteBytes::class, 'bytes')) {
            self::bindString($sql, $binding, CuteBytes::bytes((string)$value, 1024));
        } else {
            self::bindString($sql, $binding, (string)$value);
        }
    }
    
    /**
     * Bind value to parameter identifier in PDOStatement's query as bits value (string). If `\Simbiat\CuteBytes` is not available, the value will be bound as is.
     * @param \PDOStatement $sql     PDOStatement to use
     * @param string        $binding Identifier name
     * @param mixed         $value   Value to bind
     *
     * @return void
     */
    public static function bindBits(\PDOStatement $sql, string $binding, mixed $value): void
    {
        if (method_exists(CuteBytes::class, 'bytes')) {
            self::bindString($sql, $binding, CuteBytes::bytes((string)$value, 1024, bits: true));
        } else {
            self::bindString($sql, $binding, (string)$value);
        }
    }
    
    /**
     * Bind value to parameter identifier in PDOStatement's query as a string for `MATCH` operator in `FULLTEXT` search.
     * @param \PDOStatement $sql     PDOStatement to use
     * @param string        $binding Identifier name
     * @param mixed         $value   Value to bind
     *
     * @return void
     */
    public static function bindMatch(\PDOStatement $sql, string $binding, mixed $value): void
    {
        #Same as string, but for MATCH operator, when your string can have special characters, that will break the query
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
        ], '', (string)$value);
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
        self::bindString($sql, $binding, $newValue);
    }
    
    /**
     * Bind value to parameter identifier in PDOStatement's query as a string wrapped in `%` for a `LIKE` statement.
     * @param \PDOStatement $sql     PDOStatement to use
     * @param string        $binding Identifier name
     * @param mixed         $value   Value to bind
     *
     * @return void
     */
    public static function bindLike(\PDOStatement $sql, string $binding, mixed $value): void
    {
        #Same as string, but wrapped in % for LIKE '%string%'
        self::bindString($sql, $binding, '%'.$value.'%');
    }
    
    /**
     * Bind value to parameter identifier in PDOStatement's query as a binary object.
     * @param \PDOStatement $sql     PDOStatement to use
     * @param string        $binding Identifier name
     * @param mixed         $value   Value to bind
     *
     * @return void
     */
    public static function bindBinary(\PDOStatement $sql, string $binding, mixed $value): void
    {
        #Suppress warning from custom inspection, since we are dealing with binary data here, so use of mb_strlen is not appropriate
        /** @noinspection NoMBMultibyteAlternative */
        $sql->bindParam($binding, $value, \PDO::PARAM_LOB, \strlen($value));
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
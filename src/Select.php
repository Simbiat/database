<?php
declare(strict_types = 1);

namespace Simbiat\Database;

use function count;

/**
 * Useful semantic wrappers to SELECT from databases
 */
final class Select extends Query
{
    /**
     * Return full results as a multidimensional array (associative by default).
     *
     * @param string $query     Query to run
     * @param array  $bindings  List of bindings
     * @param int    $fetchMode Fetch mode
     *
     * @return array
     */
    public static function selectAll(string $query, array $bindings = [], int $fetchMode = \PDO::FETCH_ASSOC): array
    {
        try {
            if (self::isSelect($query) && self::query($query, $bindings, $fetchMode)) {
                return self::$lastResult;
            }
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to select rows with `'.$e->getMessage().'`', 0, $e);
        }
        return [];
    }
    
    /**
     * Returns only 1 row from SELECT (essentially LIMIT 1).
     *
     * @param string $query     Query to run
     * @param array  $bindings  List of bindings
     * @param int    $fetchMode Fetch mode
     *
     * @return array
     */
    public static function selectRow(string $query, array $bindings = [], int $fetchMode = \PDO::FETCH_ASSOC): array
    {
        try {
            if (self::isSelect($query)) {
                #Check if the query has a limit (any limit)
                if (preg_match('/\s*LIMIT\s+(\d+\s*,\s*)?\d+\s*;?\s*$/ui', $query) !== 1) {
                    #If it does not - add it. But first we need to remove the final semicolon if any
                    #Need to do this, because otherwise I get 2 matches, which results in 2 entries added in further preg_replace
                    #No idea how to circumvent that.
                    #Also, add LIMIT to the end of the query.
                    $query = preg_replace(['/(;?\s*\z)/mui', '/\z/mui'], ['', ' LIMIT 0, 1;'], $query);
                }
                if (self::query($query, $bindings, $fetchMode, 'row')) {
                    return self::$lastResult;
                }
            }
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to select row with `'.$e->getMessage().'`', 0, $e);
        }
        return [];
    }
    
    /**
     * Returns column (first by default) even if the original SELECT requests for more. Change the 3rd parameter accordingly to use another column as a key (starting from 0).
     * @param string $query    Query to run
     * @param array  $bindings List of bindings
     * @param int    $column   Number of the column to select
     *
     * @return array
     */
    public static function selectColumn(string $query, array $bindings = [], int $column = 0): array
    {
        try {
            if (self::isSelect($query) && self::query($query, $bindings, \PDO::FETCH_COLUMN, $column)) {
                return self::$lastResult;
            }
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to select column with `'.$e->getMessage().'`', 0, $e);
        }
        return [];
    }
    
    /**
     * Returns a value directly, instead of an array containing that value. Useful for getting specific settings from DB. No return typing, since it may vary, so be careful with that.
     * @param string $query    Query to run
     * @param array  $bindings List of bindings
     * @param int    $column   Number of the column to select
     *
     * @return mixed|null
     */
    public static function selectValue(string $query, array $bindings = [], int $column = 0): mixed
    {
        try {
            if (self::isSelect($query) && self::query($query, $bindings, \PDO::FETCH_COLUMN, $column)) {
                #We always need to take the 1st element, since our goal is to return only 1 value
                return (self::$lastResult[0] ?? NULL);
            }
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to select value with `'.$e->getMessage().'`', 0, $e);
        }
        return NULL;
    }
    
    /**
     * Returns key->value pair(s) based on 2 columns. The first column (by default) is used as a key. Change the 3rd parameter accordingly to use another column as a key (starting from 0).
     * @param string $query    Query to run
     * @param array  $bindings List of bindings
     * @param int    $column   Number of the column to select
     *
     * @return array
     */
    public static function selectPair(string $query, array $bindings = [], int $column = 0): array
    {
        try {
            if (self::isSelect($query) && self::query($query, $bindings, \PDO::FETCH_KEY_PAIR, $column)) {
                return self::$lastResult;
            }
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to select pairs with `'.$e->getMessage().'`', 0, $e);
        }
        return [];
    }
    
    /**
     * Returns unique values from a column (first by default). Change the 3rd parameter accordingly to use another column as a key (starting from 0).
     * @param string $query    Query to run
     * @param array  $bindings List of bindings
     * @param int    $column   Number of the column to select
     *
     * @return array
     */
    public static function selectUnique(string $query, array $bindings = [], int $column = 0): array
    {
        try {
            if (self::isSelect($query) && self::query($query, $bindings, \PDO::FETCH_COLUMN | \PDO::FETCH_UNIQUE, $column)) {
                return self::$lastResult;
            }
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to select unique rows with `'.$e->getMessage().'`', 0, $e);
        }
        return [];
    }
    
    /**
     * Select random row(s) or value(s) from a table
     * @param string $table  Table to select from
     * @param string $column Optional column, if you want to select only 1 column
     * @param int    $number Maximum number of rows to select
     *
     * @return array
     */
    public static function selectRandom(string $table, string $column = '', int $number = 1): array
    {
        try {
            return self::selectAll('SELECT '.(empty($column) ? '*' : '`'.$column.'`').' FROM `'.$table.'` ORDER BY RAND() LIMIT :number;', [':number' => [(max($number, 1)), 'int']]);
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to select random rows with `'.$e->getMessage().'`', 0, $e);
        }
    }
    
    /**
     * Returns count value from SELECT.
     * @param string $query    `SELECT COUNT()` query. It can have other columns, but they will be ignored.
     * @param array  $bindings List of bindings.
     *
     * @return int
     */
    public static function count(string $query, array $bindings = []): int
    {
        if (preg_match('/^\s*SELECT COUNT/mi', $query) === 1) {
            try {
                if (self::query($query, $bindings, \PDO::FETCH_COLUMN, 0)) {
                    if (empty(self::$lastResult)) {
                        return 0;
                    }
                    return (int)self::$lastResult[0];
                }
                return 0;
            } catch (\Throwable $e) {
                throw new \RuntimeException('Failed to count rows with `'.$e->getMessage().'`', 0, $e);
            }
        } else {
            throw new \UnexpectedValueException('Query is not SELECT COUNT.');
        }
    }
    
    /**
     * Returns a boolean value indicating if anything matching SELECT exists.
     * @param string $query     Query to run
     * @param array  $bindings  List of bindings
     * @param int    $fetchMode Fetch mode
     *
     * @return bool
     */
    public static function check(string $query, array $bindings = [], int $fetchMode = \PDO::FETCH_ASSOC): bool
    {
        try {
            if (self::isSelect($query) && self::query($query, $bindings, $fetchMode)) {
                return !empty(self::$lastResult);
            }
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to check if value exists with `'.$e->getMessage().'`', 0, $e);
        }
        return false;
    }
    
    /**
     * Returns an array of counts for each unique value in a column. Does not use bindings, so be careful not to process user input directly.
     * @param string $table        Table name to count in
     * @param string $columnName   Column to count
     * @param string $where        Optional WHERE condition. Full notations (`table`.`column`) are advised.
     * @param string $joinTable    Optional table to JOIN with
     * @param string $joinType     Type of JOIN to use (`INNER` by default)
     * @param string $joinOn       Column to JOIN on (defaults to the same column name we are counting)
     * @param string $joinReturn   Mandatory in case we use JOIN. If it's not set, we do not know what to GROUP by, but the original column, doing which with a JOIN will make no sense, since JOIN will be useful only to, for example, replace IDs with respective names. Full notations (`table`.`column`) are advised.
     * @param string $order        DESC (default) or ASC order the output by `count`
     * @param int    $limit        Optional limit of the output
     * @param array  $extraGroup   Optional list (array) of column names to GROUP by BEFORE the original $columnName or $joinReturn. Full notations (`table`.`column`) are advised.
     * @param bool   $altJoin      Apply JOIN logic AFTER the original COUNT. In some cases this may provide significant performance improvement, since we will be JOINing only the result, not the whole table. This approach is disabled by default, because depending on what is sent in $joinReturn and $extraGroup it may easily fail or provide unexpected results.
     * @param array  $extraColumns Optional list of additional columns to return on initial SELECT. May sometimes help with errors in the case of `$altJoin`. If this is used, you can use `tempresult` in $joinReturn.
     *
     * @return array
     */
    public static function countUnique(string $table, string $columnName, string $where = '', string $joinTable = '', string $joinType = 'INNER', string $joinOn = '', string $joinReturn = '', string $order = 'DESC', int $limit = 0, array $extraGroup = [], bool $altJoin = false, array $extraColumns = []): array
    {
        #Prevent negative LIMIT
        if ($limit < 0) {
            $limit = 0;
        }
        #Sanitize ORDER
        if (preg_match('/(DESC|ASC)/mi', $order) !== 1) {
            $order = 'DESC';
        }
        #Building query
        if ($joinTable === '') {
            $query = 'SELECT '.(empty($extraColumns) ? '' : implode(', ', $extraColumns).', ').'`'.$table.'`.`'.$columnName.'` AS `value`, count(`'.$table.'`.`'.$columnName.'`) AS `count` FROM `'.$table.'` '.($where === '' ? '' : 'WHERE '.$where.' ').'GROUP BY '.(empty($extraGroup) ? '' : implode(', ', $extraGroup).', ').'`value` ORDER BY `count` '.$order.($limit === 0 ? '' : ' LIMIT '.$limit);
        } elseif (preg_match('/(NATURAL )?((INNER|CROSS)|((LEFT|RIGHT)$)|(((LEFT|RIGHT)\s*)?OUTER))/mi', $joinType) === 1) {
            #Check if we have a setup to return after JOIN
            if (empty($joinReturn)) {
                throw new \UnexpectedValueException('No value to return after JOIN was provided.');
            }
            #Check of we have a column to join on. If not - set its name to the name of the original column
            if (empty($joinOn)) {
                $joinOn = $columnName;
            }
            if ($altJoin) {
                $query = 'SELECT '.$joinReturn.' AS `value`, `count` FROM (SELECT '.(empty($extraColumns) ? '' : implode(', ', $extraColumns).', ').'`'.$table.'`.`'.$columnName.'`, count(`'.$table.'`.`'.$columnName.'`) AS `count` FROM `'.$table.'` '.($where === '' ? '' : 'WHERE '.$where.' ').'GROUP BY '.(empty($extraGroup) ? '' : implode(', ', $extraGroup).', ').'`'.$table.'`.`'.$columnName.'` ORDER BY `count` '.$order.($limit === 0 ? '' : ' LIMIT '.$limit).') `tempresult` INNER JOIN `'.$joinTable.'` ON `tempresult`.`'.$columnName.'`=`'.$joinTable.'`.`'.$joinOn.'` ORDER BY `count` '.$order;
            } else {
                $query = 'SELECT '.$joinReturn.' AS `value`, count(`'.$table.'`.`'.$columnName.'`) AS `count` FROM `'.$table.'` INNER JOIN `'.$joinTable.'` ON `'.$table.'`.`'.$columnName.'`=`'.$joinTable.'`.`'.$joinOn.'` '.($where === '' ? '' : 'WHERE '.$where.' ').'GROUP BY '.(empty($extraGroup) ? '' : implode(', ', $extraGroup).', ').'`value` ORDER BY `count` '.$order.($limit === 0 ? '' : ' LIMIT '.$limit);
            }
        } else {
            throw new \UnexpectedValueException('Unsupported type of JOIN ('.$joinType.') was provided.');
        }
        try {
            if (self::query($query)) {
                return self::$lastResult;
            }
            return [];
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to count unique values with `'.$e->getMessage().'`', 0, $e);
        }
    }
    
    /**
     * Similar to `countUnique` but uses SUM based on comparison of the column's values against the list provided. Each `value` will be present in a separate column. In some cases your results will look like transposed countUnique, but in other cases this can provide some more flexibility in terms of how to structure them.
     * @param string $table      Table name to count in
     * @param string $columnName Column to count
     * @param array  $values     List of values to check for. Defaults to 0 and 1 (boolean)
     * @param array  $names      List of names for resulting columns. Defaults to `false` and `true`
     * @param string $where      Optional WHERE condition. Full notations (`table`.`column`) are advised.
     * @param string $joinTable  Optional table to JOIN with
     * @param string $joinType   Type of JOIN to use (`INNER` by default)
     * @param string $joinOn     Column to JOIN on (defaults to the same column name we are counting).
     * @param string $joinReturn Mandatory in case we use JOIN. If it's not set, we do not know what to GROUP by, but the original column, doing which with a JOIN will make no sense, since JOIN will be useful only to, for example, replace IDs with respective names. Full notations (`table`.`column`) are advised.
     * @param string $order      DESC (default) or ASC order the output by 1st column
     * @param int    $limit      Optional limit of the output
     * @param array  $extraGroup Optional list (array) of column names to GROUP by BEFORE the original $columnName or $joinReturn. Full notations (`table`.`column`) are advised.
     *
     * @return array
     */
    public static function sumUnique(string $table, string $columnName, array $values = [], array $names = [], string $where = '', string $joinTable = '', string $joinType = 'INNER', string $joinOn = '', string $joinReturn = '', string $order = 'DESC', int $limit = 0, array $extraGroup = []): array
    {
        #Default $values
        if (empty($values)) {
            $values = [0, 1];
        } else {
            #Ensure we use a regular array, not an associative one
            $values = array_values($values);
        }
        #Default $names
        if (empty($names)) {
            $names = ['false', 'true'];
        } else {
            #Ensure we use a regular array, not an associative one
            $names = array_values($names);
        }
        #Check that both $values and $names have the identical length
        if (count($values) !== count($names)) {
            throw new \UnexpectedValueException('Array of names provided to sumUnique function has different number of elements than array of values ('.count($names).' instead of '.count($values).')');
        }
        #Prevent negative LIMIT
        if ($limit < 0) {
            $limit = 0;
        }
        #Sanitize ORDER
        if (preg_match('/(DESC|ASC)/mi', $order) !== 1) {
            $order = 'DESC';
        }
        #Build conditional fields
        $sumFields = [];
        $bindings = [];
        foreach ($values as $key => $value) {
            $sumFields[] = 'SUM(IF(`'.$table.'`.`'.$columnName.'` = :'.$key.', 1, 0)) AS `'.$names[$key].'`';
            $bindings[':'.$key] = (string)$value;
        }
        #Building query
        if ($joinTable === '') {
            $query = 'SELECT '.implode(', ', $sumFields).' FROM `'.$table.'` '.($where === '' ? '' : 'WHERE '.$where.' ').'GROUP BY '.(empty($extraGroup) ? '' : implode(', ', $extraGroup).', ').'1 ORDER BY 1 '.$order.($limit === 0 ? '' : ' LIMIT '.$limit);
        } elseif (preg_match('/(NATURAL )?((INNER|CROSS)|((LEFT|RIGHT)$)|(((LEFT|RIGHT)\s*)?OUTER))/mi', $joinType) === 1) {
            #Check if we have a setup to return after JOIN
            if (empty($joinReturn)) {
                throw new \UnexpectedValueException('No value to return after JOIN was provided.');
            }
            #Check of we have a column to join on. If not - set its name to the name of the original column
            if (empty($joinOn)) {
                $joinOn = $columnName;
            }
            $query = 'SELECT '.$joinReturn.', '.implode(', ', $sumFields).' FROM `'.$table.'` INNER JOIN `'.$joinTable.'` ON `'.$table.'`.`'.$columnName.'`=`'.$joinTable.'`.`'.$joinOn.'` '.($where === '' ? '' : 'WHERE '.$where.' ').'GROUP BY '.(empty($extraGroup) ? '' : implode(', ', $extraGroup).', ').'1 ORDER BY 1 '.$order.($limit === 0 ? '' : ' LIMIT '.$limit);
        } else {
            throw new \UnexpectedValueException('Unsupported type of JOIN ('.$joinType.') was provided.');
        }
        try {
            if (self::query($query, $bindings)) {
                return self::$lastResult;
            }
            return [];
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to sum unique values with `'.$e->getMessage().'`', 0, $e);
        }
    }
    
}
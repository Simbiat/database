<?php
declare(strict_types=1);
namespace Simbiat\Database;

use Simbiat\SandClock;

class Controller
{
    #List of functions, that may return rows
    const selects = [
        'SELECT', 'SHOW', 'HANDLER', 'ANALYZE', 'CHECK', 'DESCRIBE', 'DESC', 'EXPLAIN', 'HELP'
    ];
    #Static for convenience, in case object gets destroyed, but you still want to get total number
    public static int $queries = 0;
    private ?\PDO $dbh;
    private bool $debug = false;
    private int $maxRunTime = 3600; #in seconds
    private int $maxTries = 5;
    private int $sleep = 5; #in seconds
    private mixed $result = NULL;

    public function __construct()
    {
        $this->dbh = (new Pool)->openConnection();
    }

    /**
     * @param string|array $queries - query/queries to run
     * @param array $bindings - global bindings, that need to be applied to all queries
     * @param int $fetch_style - FETCH type used by SELECT queries. Applicable only if 1 query is sent
     * @param mixed|null $fetch_argument - fetch mode for PDO
     * @param array $ctor_args - constructorArgs for fetchAll PDO function
     * @param bool $transaction - flag whether to use TRANSACTION mode. TRUE by default to allow more consistency
     * @return bool
     * @throws \Exception
     */
    public function query(string|array $queries, array $bindings = [], int $fetch_style = \PDO::FETCH_ASSOC, int|string|object|null $fetch_argument = NULL, array $ctor_args = [], bool $transaction = true): bool
    {
        #Check if query string was sent
        if (is_string($queries)) {
            #Convert to array
            $queries = [[$queries, $bindings]];
        } else {
            #Ensure integer keys
            $queries = array_values($queries);
            #Iterrate over array to merge binding
            foreach ($queries as $key=>$query) {
                #Ensure integer keys
                $queries[$key] = array_values($query);
                #Check if query is a string
                if (!is_string($queries[$key][0])) {
                    #Exit earlier for speed
                    throw new \UnexpectedValueException('Query #'.$key.' is not a string.');
                }
                #Merge bindings
                $queries[$key][1] = array_merge($queries[$key][1] ?? [], $bindings);
            }
        }
        #Remove any SELECT queries and comments if more than 1 query is sent
        if (count($queries) > 1) {
            foreach ($queries as $key=>$query) {
                #Check if query is SELECT
                if (preg_match('/^\s*\(*'.implode('|', self::selects).'/mi', $query[0]) === 1) {
                    unset($queries[$key]);
                    continue;
                }
                #Check if query is a comment
                if (preg_match('/^\s*(--|#|\/\*).*$/', $query[0]) === 1) {
                    unset($queries[$key]);
                }
            }
        }
        #Check if array of queries is empty
        if (empty($queries)) {
            #Issue a notice
            trigger_error('No queries were provided to `query()` function', E_USER_NOTICE);
            #Do not consider this an "error" by default and return `true`
            return true;
        }
        #Flag for SELECT, used as sort of "cache" instead of counting values every time
        $select = false;
        #If we have just 1 query, disable transaction
        if (count($queries) === 1) {
            $transaction = false;
            if (preg_match('/^\s*\(*'.implode('|', self::selects).'/mi', $queries[0][0]) === 1) {
                $select = true;
            }
        }
        #Check if we are running a SELECT
        if (!$select) {
            #If not - use $result as counter for number of affected rows and reset it before run
            $this->result = 0;
        }
        #Set counter for tries
        $try = 0;
        do {
            try {
                #Indicate actual try
                $try++;
                #Initiate transaction, if we are using it
                if ($transaction) {
                    $this->dbh->beginTransaction();
                }
                #Loop through queries
                foreach ($queries as $key=>$query) {
                    #Prepare query
                    $sql = $this->dbh->prepare($query[0]);
                    #Bind values, if any
                    if (!empty($query[1])) {
                        $sql = $this->binding($sql, $query[1]);
                    }
                    #Increasing time limit for potentially long operations (like optimize)
                    set_time_limit($this->maxRunTime);
                    #Increase the number of queries
                    self::$queries++;
                    #Execute the query
                    $sql->execute();
                    #If debug is enabled dump PDO details
                    if ($this->debug) {
                        $sql->debugDumpParams();
                        ob_flush();
                        flush();
                    }
                    if ($select) {
                        #Adjust fetching mode
                        if ($fetch_argument === 'row') {
                            $this->result = $sql->fetch($fetch_style);
                        } elseif (in_array($fetch_style, [\PDO::FETCH_COLUMN, \PDO::FETCH_FUNC, \PDO::FETCH_INTO])) {
                            $this->result = $sql->fetchAll($fetch_style, $fetch_argument);
                        } elseif ($fetch_style === \PDO::FETCH_CLASS) {
                            $this->result = $sql->fetchAll($fetch_style, $fetch_argument, $ctor_args);
                        } else {
                            $this->result = $sql->fetchAll($fetch_style);
                        }
                    } else {
                        #Increase counter of affected rows (inserted, deleted, updated)
                        $this->result += $sql->rowCount();
                    }
                    #Remove the query from the bulk, if not using transaction mode, to avoid repeating of commands
                    if (!$transaction) {
                        unset($queries[$key]);
                    }
                }
                #Initiate transaction, if we are using it
                if ($transaction) {
                    $this->dbh->commit();
                }
                return true;
            } catch (\Exception $e) {
                $error = $e->getMessage().$e->getTraceAsString();
                if (isset($sql) && $this->debug) {
                    $sql->debugDumpParams();
                    echo $error;
                    ob_flush();
                    flush();
                }
                #Check if deadlock
                if (isset($sql) && ($sql->errorCode() == '40001' || preg_match('/.*(deadlock|try restarting transaction).*/mis', $error) === 1 )) {
                    $deadlock = true;
                    if ($try == $this->maxTries) {
                        error_log($error);
                    }
                } else {
                    $deadlock = false;
                    error_log($error);
                }
                if ($this->dbh->inTransaction()) {
                    $this->dbh->rollBack();
                    if (!$deadlock) {
                        throw $e;
                    }
                }
                #If deadlock - sleep and then retry
                if ($deadlock) {
                    sleep($this->sleep);
                    continue;
                } else {
                    throw $e;
                }
            }
        } while ($try <= $this->maxTries);
        throw new \Exception('Deadlock encountered for set maximum of '.$this->maxTries.' tries.');
    }

    #Function mainly for convenience and some types enforcing, which sometimes 'fail' in PDO itself
    private function binding(\PDOStatement $sql, array $bindings = []): \PDOStatement
    {
        foreach ($bindings as $binding=>$value) {
            if (!is_array($value)) {
                $sql->bindValue($binding, $value);
            } else {
                switch(strtolower($value[1])) {
                    case 'date':
                        $sql->bindValue($binding, $this->time($value[0], 'Y-m-d'));
                        break;
                    case 'time':
                        $sql->bindValue($binding, $this->time($value[0]));
                        break;
                    case 'bool':
                    case 'boolean':
                        $sql->bindValue($binding, boolval($value[0]), \PDO::PARAM_BOOL);
                        break;
                    case 'null':
                        $sql->bindValue($binding, NULL, \PDO::PARAM_NULL);
                        break;
                    case 'int':
                    case 'integer':
                    case 'number':
                    case 'limit':
                    case 'offset':
                        $sql->bindValue($binding, intval($value[0]), \PDO::PARAM_INT);
                        break;
                    case 'str':
                    case 'string':
                    case 'text':
                    case 'float':
                    case 'varchar':
                    case 'varchar2':
                        $sql->bindValue($binding, strval($value[0]));
                        break;
                    case 'match':
                        #Same as string, but for MATCH operator, when your string can have special characters, that will break the query
                        #Trim first
                        $newValue = preg_replace('/^\p{Z}+|\p{Z}+$/u', '', strval($value[0]));
                        #Remove all symbols except allowed operators and space. @distance is not included, since it's unlikely a human will be using it through UI form
                        $newValue = preg_replace('/[^\p{L}\p{N}_+\-<>~()"* ]/u', '', $newValue);
                        #Remove all operators, that can only precede a text and that are not preceded by either beginning of string or space
                        $newValue = preg_replace('/(?<!^| )[+\-<>~]/u', '', $newValue);
                        #Remove all double quotes and asterisks, that are not preceded by either beginning of string, letter, number or space
                        $newValue = preg_replace('/(?<![\p{L}\p{N}_ ]|^)[*"]/u', '', $newValue);
                        #Remove all double quotes and asterisks, that are inside text
                        $newValue = preg_replace('/([\p{L}\p{N}_])([*"])([\p{L}\p{N}_])/u', '', $newValue);
                        #Remove all opening parenthesis which are not preceded by beginning of string or space
                        $newValue = preg_replace('/(?<!^| )\(/u', '', $newValue);
                        #Remove all closing parenthesis which are not preceded by beginning of string or space or are not followed by end of string or space
                        $newValue = preg_replace('/(?<![\p{L}\p{N}_])\)|\)(?! |$)/u', '', $newValue);
                        #Remove all double quotes if the count is not even
                        if (substr_count($newValue, '"') % 2 !== 0) {
                            $newValue = preg_replace('/"/u', '', $newValue);
                        }
                        #Remove all parenthesis if count of closing does not match count of opening ones
                        if (substr_count($newValue, '(') !== substr_count($newValue, ')')) {
                            $newValue = preg_replace('/[()]/u', '', $newValue);
                        }
                        #Check if the new value is just the set of operators and if it is - set the value to an empty string
                        if (preg_match('/[+\-<>~()"*]+/u', $newValue)) {
                            $newValue = '';
                        }
                        $sql->bindValue($binding, $newValue);
                        break;
                    case 'like':
                        #Same as string, but wrapped in % for LIKE '%string%'
                        $sql->bindValue($binding, '%'.$value[0].'%');
                        break;
                    case 'lob':
                    case 'large':
                    case 'object':
                    case 'blob':
                        $sql->bindParam($binding, $value[0], \PDO::PARAM_LOB, strlen($value[0]));
                        break;
                    case 'like':
                        $sql->bindValue($binding, '%'.$value[0].'%');
                        break;
                    default:
                        if (is_int($value[1])) {
                            $sql->bindValue($binding, $value[0], $value[1]);
                        } else {
                            $sql->bindValue($binding, strval($value[0]));
                        }
                }
            }
        }
        return $sql;
    }

    private function time(string|float|int $time = 0, string $format = 'Y-m-d H:i:s.u'): string
    {
        return (new SandClock)->setFormat($format)->format($time);
    }

    ##########################
    #Useful semantic wrappers#
    ##########################
    #Return full results as multidimensional array (associative by default).
    /**
     * @throws \Exception
     */
    public function selectAll(string $query, array $bindings = [], int $fetchMode = \PDO::FETCH_ASSOC): array
    {
        if ($this->isSelect($query) === true) {
            self::$queries++;
            if ($this->query($query, $bindings, $fetchMode) && is_array($this->getResult())) {
                return $this->getResult();
            }
        }
        return [];
    }

    #Returns only 1 row from SELECT (essentially LIMIT 1).
    /**
     * @throws \Exception
     */
    public function selectRow(string $query, array $bindings = [], int $fetchMode = \PDO::FETCH_ASSOC): array
    {
        if ($this->isSelect($query) === true) {
            self::$queries++;
            if ($this->query($query, $bindings, $fetchMode, 'row') && is_array($this->getResult())) {
                return $this->getResult();
            }
        }
        return [];
    }

    #Returns column (first by default) even if original SELECT requests for more. Change 3rd parameter accordingly to use another column as key (starting from 0).
    /**
     * @throws \Exception
     */
    public function selectColumn(string $query, array $bindings = [], int $column = 0): array
    {
        if ($this->isSelect($query) === true) {
            self::$queries++;
            if ($this->query($query, $bindings, \PDO::FETCH_COLUMN, $column) && is_array($this->getResult())) {
                return $this->getResult();
            }
        }
        return [];
    }

    #Returns a value directly, instead of array containing that value. Useful for getting specific settings from DB. No return typing, since it may vary, so be careful with that.
    /**
     * @throws \Exception
     */
    public function selectValue(string $query, array $bindings = [], int $column = 0)
    {
        if ($this->isSelect($query) === true) {
            self::$queries++;
            if ($this->query($query, $bindings, \PDO::FETCH_COLUMN, $column) && is_array($this->getResult())) {
                return ($this->getResult()[$column] ?? NULL);
            }
        }
        return NULL;
    }

    #Returns key->value pair(s) based on 2 columns. First column (by default) is used as key. Change 3rd parameter accordingly to use another column as key (starting from 0).
    /**
     * @throws \Exception
     */
    public function selectPair(string $query, array $bindings = [], int $column = 0): array
    {
        if ($this->isSelect($query) === true) {
            self::$queries++;
            if ($this->query($query, $bindings, \PDO::FETCH_KEY_PAIR, $column) && is_array($this->getResult())) {
                return $this->getResult();
            }
        }
        return [];
    }

    #Returns unique values from a column (first by default). Change 3rd parameter accordingly to use another column as key (starting from 0).
    /**
     * @throws \Exception
     */
    public function selectUnique(string $query, array $bindings = [], int $column = 0): array
    {
        if ($this->isSelect($query) === true) {
            self::$queries++;
            if ($this->query($query, $bindings, \PDO::FETCH_COLUMN|\PDO::FETCH_UNIQUE, $column) && is_array($this->getResult())) {
                return $this->getResult();
            }
        }
        return [];
    }

    #Select random row from table
    /**
     * @throws \Exception
     */
    public function selectRandom(string $table, string $column = '', int $number = 1): array
    {
        return $this->selectAll('SELECT '.(empty($column) ? '*' : '`'.$column.'`').' FROM `'.$table.'` ORDER BY RAND() LIMIT :number;', [':number'=>[($number >= 1 ? $number : 1), 'int']]);
    }

    #Returns count value from SELECT.
    /**
     * @throws \Exception
     */
    public function count(string $query, array $bindings = []): int
    {
        if (preg_match('/^\s*SELECT COUNT/mi', $query) === 1) {
            self::$queries++;
            if ($this->query($query, $bindings, \PDO::FETCH_COLUMN, 0) && is_array($this->getResult())) {
                if (empty($this->getResult())) {
                    return 0;
                } else {
                    return intval($this->getResult()[0]);
                }
            } else {
                return 0;
            }
        } else {
            throw new \UnexpectedValueException('Query is not SELECT COUNT.');
        }
    }

    #Returns boolean value indicating, if anything matching SELECT exists.
    /**
     * @throws \Exception
     */
    public function check(string $query, array $bindings = [], int $fetchMode = \PDO::FETCH_ASSOC): bool
    {
        if ($this->isSelect($query) === true) {
            self::$queries++;
            if ($this->query($query, $bindings, $fetchMode) && is_array($this->getResult()) && !empty($this->getResult())) {
                return true;
            }
        }
        return false;
    }

    #Check if a table exists
    /**
     * @throws \Exception
     */
    public function checkTable(string $table, string $schema = ''): bool
    {
        #Adjust query depending on whether schema is set
        if (empty($schema)) {
            $query = 'SELECT `TABLE_NAME` FROM `information_schema`.`TABLES` WHERE `TABLE_NAME` = :table;';
            $bindings = [':table' => $table];
        } else {
            $query = 'SELECT `TABLE_NAME` FROM `information_schema`.`TABLES` WHERE `TABLE_NAME` = :table AND `TABLE_SCHEMA` = :schema;';
            $bindings = [':table' => $table, ':schema' => $schema];
        }
        return $this->check($query, $bindings);
    }

    #Check if a column exists in a table
    /**
     * @throws \Exception
     */
    public function checkColumn(string $table, string $column, string $schema = ''): bool
    {
        #Adjust query depending on whether schema is set
        if (empty($schema)) {
            $query = 'SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE `TABLE_NAME` = :table AND `COLUMN_NAME` = :column;';
            $bindings = [':table' => $table, ':column' => $column];
        } else {
            $query = 'SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE `TABLE_NAME` = :table AND `COLUMN_NAME` = :column AND `TABLE_SCHEMA` = :schema;';
            $bindings = [':table' => $table, ':column' => $column, ':schema' => $schema];
        }
        return $this->check($query, $bindings);
    }

    #Returns array of counts  for each unique value in column. Does not use bindings, so be careful not to process user input directly.
    #$table - table name to count in
    #$columnName - column to count
    #$where - optional WHERE condition. Full notations (`table`.`column`) are advised.
    #$joinTable - optional table to JOIN with
    #$joinType - type of JOIN to use
    #$joinOn - column to JOIN on (defaults to the same column name we are counting).
    #$joinReturn - mandatory in case we use JOIN. If it's not set we do not know what to GROUP by but the original column, doing which with a JOIN will make no sense, since JOIN will be useful only to, for example, replace IDs with respective names. Full notations (`table`.`column`) are advised.
    #$order - DESC or ASC order the output by `count`
    #$limit - optional limit of the output
    #$extraGroup - optional list (array) of column names to GROUP by BEFORE the original $columnName or $joinReturn. Full notations (`table`.`column`) are advised.
    #$altJoin - apply JOIN logic AFTER the original COUNT. In some cases this may provide significant performance improvement, since we will be JOINing only the result, not the whole table. This approach is disabled by default, because depending on what is sent in $joinReturn and $extraGroup it may easily fail or provide unexpected results.
    #$extraColumns - optional list of additional columns to return on initial SELECT. May sometimes help with errors in case of $altJoin. If this is used you can use `tempresult` in $joinReturn.
    /**
     * @throws \Exception
     */
    public function countUnique(string $table, string $columnName, string $where = '', string $joinTable = '', string $joinType = 'INNER', string $joinOn = '', string $joinReturn = '', string $order = 'DESC', int $limit = 0, array $extraGroup = [], bool $altJoin = false, array $extraColumns = []): array
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
        } else {
            #Check for proper JOIN type
            if (preg_match('/(NATURAL )?((INNER|CROSS)|((LEFT|RIGHT)$)|(((LEFT|RIGHT)\s*)?OUTER))/mi', $joinType) === 1) {
                #Check if we have a setup to return after JOIN
                if (empty($joinReturn)) {
                    throw new \UnexpectedValueException('No value to return after JOIN was provided.');
                }
                #Check of we have a column to join on. If not - set its name to the name of original column
                if (empty($joinOn)) {
                    $joinOn = $columnName;
                }
                if ($altJoin === false) {
                    $query = 'SELECT '.$joinReturn.' AS `value`, count(`'.$table.'`.`'.$columnName.'`) AS `count` FROM `'.$table.'` INNER JOIN `'.$joinTable.'` ON `'.$table.'`.`'.$columnName.'`=`'.$joinTable.'`.`'.$joinOn.'` '.($where === '' ? '' : 'WHERE '.$where.' ').'GROUP BY '.(empty($extraGroup) ? '' : implode(', ', $extraGroup).', ').'`value` ORDER BY `count` '.$order.($limit === 0 ? '' : ' LIMIT '.$limit);
                } else {
                    $query = 'SELECT '.$joinReturn.' AS `value`, `count` FROM (SELECT '.(empty($extraColumns) ? '' : implode(', ', $extraColumns).', ').'`'.$table.'`.`'.$columnName.'`, count(`'.$table.'`.`'.$columnName.'`) AS `count` FROM `'.$table.'` '.($where === '' ? '' : 'WHERE '.$where.' ').'GROUP BY '.(empty($extraGroup) ? '' : implode(', ', $extraGroup).', ').'`'.$table.'`.`'.$columnName.'` ORDER BY `count` '.$order.($limit === 0 ? '' : ' LIMIT '.$limit).') `tempresult` INNER JOIN `'.$joinTable.'` ON `tempresult`.`'.$columnName.'`=`'.$joinTable.'`.`'.$joinOn.'` ORDER BY `count` '.$order;
                }
            } else {
                throw new \UnexpectedValueException('Unsupported type of JOIN ('.$joinType.') was provided.');
            }
        }
        self::$queries++;
        if ($this->query($query) && is_array($this->getResult())) {
            return $this->getResult();
        } else {
            return [];
        }
    }

    #Similar to countUnique, but utilizes SUM based on comparison of the column's values against the list provided. Each `value` will be present in a separate column. In some cases you results will look like transposed countUnique, but in other cases this can provide some more flexibility in terms of how to structure them.
    #$table - table name to count in
    #$columnName - column to count
    #$values - list of values to check for. Defaults to 0 and 1 (boolean)
    #$names - list of names for resulting columns. Defaults to `false` and `true`
    #$where - optional WHERE condition. Full notations (`table`.`column`) are advised.
    #$joinTable - optional table to JOIN with
    #$joinType - type of JOIN to use
    #$joinOn - column to JOIN on (defaults to the same column name we are counting).
    #$joinReturn - mandatory in case we use JOIN. If it's not set we do not know what to GROUP by but the original column, doing which with a JOIN will make no sense, since JOIN will be useful only to, for example, replace IDs with respective names. Full notations (`table`.`column`) are advised.
    #$order - DESC or ASC order the output by 1 (that is 1st column in SELECT)
    #$limit - optional limit of the output
    #$extraGroup - optional list (array) of column names to GROUP by BEFORE the original $columnName or $joinReturn. Full notations (`table`.`column`) are advised.
    /**
     * @throws \Exception
     */
    public function sumUnique(string $table, string $columnName, array $values = [], array $names = [], string $where = '', string $joinTable = '', string $joinType = 'INNER', string $joinOn = '', string $joinReturn = '', string $order = 'DESC', int $limit = 0, array $extraGroup = []): array
    {
        #Default $values
        if (empty($values) === true) {
            $values = [0, 1];
        } else {
            #Ensure we use regular array, not associative one
            $values = array_values($values);
        }
        #Default $names
        if (empty($names) === true) {
            $names = ['false', 'true'];
        } else {
            #Ensure we use regular array, not associative one
            $names = array_values($names);
        }
        #Check that both $values and $names have identical length
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
        foreach ($values as $key=>$value) {
            $sumFields[] = 'SUM(IF(`'.$table.'`.`'.$columnName.'` = :'.$key.', 1, 0)) AS `'.$names[$key].'`';
            $bindings[':'.$key] = strval($value);
        }
        #Building query
        if ($joinTable === '') {
            $query = 'SELECT '.implode(', ', $sumFields).' FROM `'.$table.'` '.($where === '' ? '' : 'WHERE '.$where.' ').'GROUP BY '.(empty($extraGroup) ? '' : implode(', ', $extraGroup).', ').'1 ORDER BY 1 '.$order.($limit === 0 ? '' : ' LIMIT '.$limit);
        } else {
            #Check for proper JOIN type
            if (preg_match('/(NATURAL )?((INNER|CROSS)|((LEFT|RIGHT)$)|(((LEFT|RIGHT)\s*)?OUTER))/mi', $joinType) === 1) {
                #Check if we have a setup to return after JOIN
                if (empty($joinReturn)) {
                    throw new \UnexpectedValueException('No value to return after JOIN was provided.');
                }
                #Check of we have a column to join on. If not - set its name to the name of original column
                if (empty($joinOn)) {
                    $joinOn = $columnName;
                }
                $query = 'SELECT '.$joinReturn.', '.implode(', ', $sumFields).' FROM `'.$table.'` INNER JOIN `'.$joinTable.'` ON `'.$table.'`.`'.$columnName.'`=`'.$joinTable.'`.`'.$joinOn.'` '.($where === '' ? '' : 'WHERE '.$where.' ').'GROUP BY '.(empty($extraGroup) ? '' : implode(', ', $extraGroup).', ').'1 ORDER BY 1 '.$order.($limit === 0 ? '' : ' LIMIT '.$limit);
            } else {
                throw new \UnexpectedValueException('Unsupported type of JOIN ('.$joinType.') was provided.');
            }
        }
        self::$queries++;
        if ($this->query($query, $bindings) && is_array($this->getResult())) {
            return $this->getResult();
        } else {
            return [];
        }
    }

    #Helper function to allow splitting a string into array of queries
    #Regexp taken from https://stackoverflow.com/questions/24423260/split-sql-statements-in-php-on-semicolons-but-not-inside-quotes
    public function stringToQueries(string $string): array
    {
        return preg_split('~\([^)]*\)(*SKIP)(*FAIL)|(?<=;)(?![ ]*$)~', $string);
    }

    #Helper function to check if query is a select(able) one
    private function isSelect(string $query): bool
    {
        if (preg_match('/^\s*\(*'.implode('|', self::selects).'/mi', $query) === 1) {
            return true;
        } else {
            throw new \UnexpectedValueException('Query is not one of '.implode(', ', self::selects).'.');
        }
    }

    #####################
    #Setters and getters#
    #####################
    public function getMaxTime(): int
    {
        return $this->maxRunTime;
    }

    public function setMaxTime(int $seconds): self
    {
        $this->maxRunTime = $seconds;
        return $this;
    }

    public function getDebug(): bool
    {
        return $this->debug;
    }

    public function setDebug(bool $debug): self
    {
        $this->debug = $debug;
        return $this;
    }

    public function getTries(): int
    {
        return $this->maxTries;
    }

    public function setTries(int $tries): self
    {
        $this->maxTries = abs($tries);
        return $this;
    }

    public function getSleep(): int
    {
        return $this->sleep;
    }

    public function setSleep(int $sleep): self
    {
        $this->sleep = abs($sleep);
        return $this;
    }

    public function getResult()
    {
        return $this->result;
    }
}

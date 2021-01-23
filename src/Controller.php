<?php
declare(strict_types=1);
namespace SimbiatDB;

class Controller
{    
    #List of functions, that may return rows
    const selects = [
        'SELECT', 'SHOW', 'HANDLER', 'ANALYZE', 'CHECK', 'DESCRIBE', 'DESC', 'EXPLAIN', 'HELP'
    ];
    #Static for convinience, in case object gets destroyed, but you still want to get total number
    public static $queries = 0;
    private object $dbh;
    private bool $debug = false;
    private int $maxruntime = 3600; #in seconds
    private int $maxtries = 5;
    private int $sleep = 5; #in seconds
    private $result = NULL;
    
    public function __construct()
    {
        $this->dbh = (new \SimbiatDB\Pool)->openConnection();
    }
    
    public function query($queries, array $bindings = [], $fetch_style = \PDO::FETCH_ASSOC, $fetch_argument = NULL, array $ctor_args = []): bool
    {
        $try = 0;
        do {
            try {
                $try++;
                if (is_string($queries)) {
                    $sql = $this->dbh->prepare($queries);
                    #Preparing bindings
                    $sql = $this->binding($sql, $bindings);
                    set_time_limit($this->maxruntime);
                    if ($this->debug) {
                        echo $queries.'<br>';
                        ob_flush();
                        flush();
                    }
                    $sql->execute();
                    self::$queries++;
                    if ($this->debug) {
                        $sql->debugDumpParams();
                        ob_flush();
                        flush();
                    }
                    if (preg_match('/^\s*\(*'.implode('|', self::selects).'/mi', $queries) === 1) {
                        if ($fetch_argument === 'row') {
                            $this->result = $sql->fetch($fetch_style);
                        } elseif ($fetch_style === \PDO::FETCH_COLUMN || $fetch_style === \PDO::FETCH_FUNC) {
                            $this->result = $sql->fetchAll($fetch_style, $fetch_argument);
                        } elseif ($fetch_style === \PDO::FETCH_CLASS) {
                            $this->result = $sql->fetchAll($fetch_style, $fetch_argument, $ctor_args);
                        } else {
                            $this->result = $sql->fetchAll($fetch_style);
                        }
                    } else {
                        $this->result = $sql->rowCount();
                    }
                    return true;
                } else {
                    if (!is_array($queries)) {
                        throw new \UnexpectedValueException('Queries sent are neither string nor array.');
                    }
                    $this->dbh->beginTransaction();
                    foreach ($queries as $sequence=>$query) {
                        if (is_string($query)) {
                            $actualquery = $query;
                        } else {
                            if (is_array($query)) {
                                if (!is_string($query[0])) {
                                    throw new \UnexpectedValueException('Query #'.$sequence.' in bulk is not a string.');
                                } else {
                                    $actualquery = $query[0];
                                }
                            } else {
                                if (!is_string($query)) {
                                    throw new \UnexpectedValueException('Query #'.$sequence.' in bulk is not a string.');
                                }
                            }
                        }
                        #Check if it's a command which may return rows
                        if (preg_match('/(^('.implode('|', self::selects).'))/i', $actualquery) === 1 && preg_match('/^SELECT.*FOR UPDATE$/mi', $actualquery) !== 1) {
                            trigger_error('A selector command ('.implode(', ', self::selects).') detected in bulk of queries. Output wll not be fetched and may result in errors in further queries. Consider revising: '.$actualquery);
                        }
                        #Check if it's a comment and skip it
                        if (preg_match('/^\s*(--|#|\/\*).*$/', $actualquery) === 1) {
                            continue;
                        }
                        $sql = $this->dbh->prepare($actualquery);
                        #Preparing bindings
                        if (is_array($query)) {
                            if (!empty($query[1])) {
                                if (is_array($query[1])) {
                                    $sql = $this->binding($sql, array_merge($bindings, $query[1]));
                                } else {
                                    throw new \UnexpectedValueException('Bindings provided for query #'.$sequence.' are not an array.');
                                }
                            }
                        }
                        #Increasing time limit for potentially long operations (like optimize)
                        set_time_limit($this->maxruntime);
                        if ($this->debug) {
                            echo $actualquery.'<br>';
                            ob_flush();
                            flush();
                        }
                        $sql->execute();
                        self::$queries++;
                        if ($this->debug) {
                            $sql->debugDumpParams();
                            ob_flush();
                            flush();
                        }
                    }
                    $this->dbh->commit();
                    return true;
                }
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
                    if ($try == $this->maxtries) {
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
            break;
        } while ($try <= $this->maxtries);
        throw new \Exception('Deadlock encountered for set maximum of '.$this->maxtries.' tries.');
    }
    
    #Function mainly for convinience and some types enforcing, which sometimes 'fail' in PDO itself
    private function binding(\PDOStatement $sql, array $bindings = []): \PDOStatement
    {
        foreach ($bindings as $binding=>$value) {
            if (!is_array($value)) {
                $sql->bindValue($binding, $value);
            } else {
                switch(strtolower($value[1])) {
                    case 'date':
                        $sql->bindValue($binding, $this->time($value[0], 'Y-m-d'), \PDO::PARAM_STR);
                        break;
                    case 'time':
                        $sql->bindValue($binding, $this->time($value[0], 'Y-m-d H:i:s.u'), \PDO::PARAM_STR);
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
                        $sql->bindValue($binding, strval($value[0]), \PDO::PARAM_STR);
                        break;
                    case 'lob':
                    case 'large':
                    case 'object':
                    case 'blob':
                        $sql->bindParam($binding, $value[0], \PDO::PARAM_LOB, strlen($value[0]));
                        break;
                    case 'like':
                        $sql->bindValue($binding, '%'.$value[0].'%', \PDO::PARAM_STR);
                        break;
                    default:
                        if (is_int($value[1])) {
                            $sql->bindValue($binding, $value[0], $value[1]);
                        } else {
                            $sql->bindValue($binding, strval($value[0]), \PDO::PARAM_STR);
                        }
                }
            }
        }
        return $sql;
    }
    
    private function time($time = 0, string $format = 'Y-m-d H:i:s.u'): string
    {
        return (new \SandClock\Api)->setFormat($format)->format($time);
    }
    
    ##########################
    #Useful semantic wrappers#
    ##########################
    #Return full results as multidimensional array (associative by default).
    public function selectAll(string $query, array $bindings = [], $fetchmode = \PDO::FETCH_ASSOC): array
    {
        if (preg_match('/^\s*\(*'.implode('|', self::selects).'/mi', $query) === 1) {
            if ($this->query($query, $bindings, $fetchmode) && is_array($this->getResult())) {
                return $this->getResult();
            } else {
                return [];
            }
        } else {
            throw new \UnexpectedValueException('Query is not one of '.implode(', ', self::selects).'.');
        }
    }
    
    #Returns only 1 row from SELECT (essentially LIMIT 1).
    public function selectRow(string $query, array $bindings = [], $fetchmode = \PDO::FETCH_ASSOC): array
    {
        if (preg_match('/^\s*\(*'.implode('|', self::selects).'/mi', $query) === 1) {
            if ($this->query($query, $bindings, $fetchmode, 'row') && is_array($this->getResult())) {
                return $this->getResult();
            } else {
                return [];
            }
        } else {
            throw new \UnexpectedValueException('Query is not one of '.implode(', ', self::selects).'.');
        }
    }
    
    #Returns column (frist by default) even if original SELECT requests for more. Change 3rd parameter accordingly to use another column as key (starting from 0).
    public function selectColumn(string $query, array $bindings = [], int $column = 0): array
    {
        if (preg_match('/^\s*\(*'.implode('|', self::selects).'/mi', $query) === 1) {
            if ($this->query($query, $bindings, \PDO::FETCH_COLUMN, $column) && is_array($this->getResult())) {
                return $this->getResult();
            } else {
                return [];
            }
        } else {
            throw new \UnexpectedValueException('Query is not one of '.implode(', ', self::selects).'.');
        }
    }
    
    #Returns a value directly, instead of array containing that value. Useful for getting specific settings from DB. No return typing, since it may vary, so be careful with that.
    public function selectValue(string $query, array $bindings = [], int $column = 0)
    {
        if (preg_match('/^\s*\(*'.implode('|', self::selects).'/mi', $query) === 1) {
            if ($this->query($query, $bindings, \PDO::FETCH_COLUMN, $column) && is_array($this->getResult())) {
                return ($this->getResult()[$column] ?? NULL);
            } else {
                return NULL;
            }
        } else {
            throw new \UnexpectedValueException('Query is not one of '.implode(', ', self::selects).'.');
        }
    }
    
    #Returns key->value pair(s) based on 2 columns. First column (by default) is used as key. Change 3rd parameter accordingly to use another column as key (starting from 0).
    public function selectPair(string $query, array $bindings = [], int $column = 0): array
    {
        if (preg_match('/^\s*\(*'.implode('|', self::selects).'/mi', $query) === 1) {
            if ($this->query($query, $bindings, \PDO::FETCH_KEY_PAIR, $column) && is_array($this->getResult())) {
                return $this->getResult();
            } else {
                return [];
            }
        } else {
            throw new \UnexpectedValueException('Query is not one of '.implode(', ', self::selects).'.');
        }
    }
    
    #Returns unique values from a column (first by default). Change 3rd parameter accordingly to use another column as key (starting from 0).
    public function selectUnique(string $query, array $bindings = [], int $column = 0): array
    {
        if (preg_match('/^\s*\(*'.implode('|', self::selects).'/mi', $query) === 1) {
            if ($this->query($query, $bindings, \PDO::FETCH_COLUMN|\PDO::FETCH_UNIQUE, $column) && is_array($this->getResult())) {
                return $this->getResult();
            } else {
                return [];
            }
        } else {
            throw new \UnexpectedValueException('Query is not one of '.implode(', ', self::selects).'.');
        }
    }
    
    #Returns count value from SELECT.
    public function count(string $query, array $bindings = array()): int
    {
        if (preg_match('/^\s*SELECT COUNT/mi', $query) === 1) {
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
            throw new \UnexpectedValueException('Query is not one of '.implode(', ', self::selects).' COUNT.');
        }
    }
    
    #Returns boolean value indicating, if anything matching SELECT exists.
    public function check(string $query, array $bindings = [], $fetchmode = \PDO::FETCH_ASSOC): bool
    {
        if (preg_match('/^\s*\(*'.implode('|', self::selects).'/mi', $query) === 1) {
            if ($this->query($query, $bindings, $fetchmode) && is_array($this->getResult()) && !empty($this->getResult())) {
                return true;
            } else {
                return false;
            }
        } else {
            throw new \UnexpectedValueException('Query is not one of '.implode(', ', self::selects).'.');
        }
    }
    
    #Returns array of counts  for each unique value in column. Does not use bindings, so be careful not to process user input directly.
    #$table - table name to count in
    #$columnname - column to count
    #$where - optional WHERE condition. Full notations (`table`.`column`) are advised.
    #$jointable - optional table to JOIN with
    #$jointype - type of JOIN to use
    #$joinon - column to JOIN on (defaults to the same column name we are counting).
    #$joinreturn - mandatory in case we use JOIN. If it's not set we do not know what to GROUP by but the original column, doing which with a JOIN will make no sense, since JOIN will be useful only to, for example, replace IDs with respective names. Full notations (`table`.`column`) are advised.
    #$order - DESC or ASC order the output by `count`
    #$limit - optional limit of the output
    #$extragroup - optional list (array) of column names to GROUP by BEFORE the original $columnname or $joinreturn. Full notations (`table`.`column`) are advised.
    #$altjoin - apply JOIN logic AFTER the original COUNT. In some cases this may provide signifficant performance improvement, since we will be JOINing only a the result, not the whole table. This approach is disabled by default, because depdending on what is sent in $joinreturn and $extragroup it may easily fail or provide unexpected results.
    #$extracolumns - optional list of additional columns to return on initial SELECT. May sometimes help with errors in case of $altjoin. If this is used you can use `tempresult` in $joinreturn.
    public function countUnique(string $table, string $columnname, string $where = '', string $jointable = '', string $jointype = 'INNER', string $joinon = '', string $joinreturn = '', string $order = 'DESC', int $limit = 0, array $extragroup = [], bool $altjoin = false, array $extracolumns = []): array
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
        if ($jointable === '') {
            $query = 'SELECT '.(empty($extracolumns) ? '' : implode(', ', $extracolumns).', ').'`'.$table.'`.`'.$columnname.'` AS `value`, count(`'.$table.'`.`'.$columnname.'`) AS `count` FROM `'.$table.'` '.($where === '' ? '' : 'WHERE '.$where.' ').'GROUP BY '.(empty($extragroup) ? '' : implode(', ', $extragroup).', ').'`value` ORDER BY `count` '.$order.($limit === 0 ? '' : ' LIMIT '.$limit);
        } else {
            #Check for proper JOIN type
            if (preg_match('/(NATURAL )?((INNER|CROSS)|((LEFT|RIGHT)$)|(((LEFT|RIGHT)\s*)?OUTER))/mi', $jointype) === 1) {
                #Check if we have a setup to return after JOIN
                if (empty($joinreturn)) {
                    throw new \UnexpectedValueException('No value to reutrn after JOIN was provided.');
                }
                #Check of we have a column to join on. If not - set its name to the name of original column
                if (empty($joinon)) {
                    $joinon = $columnname;
                }
                if ($altjoin === false) {
                    $query = 'SELECT '.$joinreturn.' AS `value`, count(`'.$table.'`.`'.$columnname.'`) AS `count` FROM `'.$table.'` INNER JOIN `'.$jointable.'` ON `'.$table.'`.`'.$columnname.'`=`'.$jointable.'`.`'.$joinon.'` '.($where === '' ? '' : 'WHERE '.$where.' ').'GROUP BY '.(empty($extragroup) ? '' : implode(', ', $extragroup).', ').'`value` ORDER BY `count` '.$order.($limit === 0 ? '' : ' LIMIT '.$limit);
                } else {
                    $query = 'SELECT '.$joinreturn.' AS `value`, `count` FROM (SELECT '.(empty($extracolumns) ? '' : implode(', ', $extracolumns).', ').'`'.$table.'`.`'.$columnname.'`, count(`'.$table.'`.`'.$columnname.'`) AS `count` FROM `'.$table.'` '.($where === '' ? '' : 'WHERE '.$where.' ').'GROUP BY '.(empty($extragroup) ? '' : implode(', ', $extragroup).', ').'`'.$table.'`.`'.$columnname.'` ORDER BY `count` '.$order.($limit === 0 ? '' : ' LIMIT '.$limit).') `tempresult` INNER JOIN `'.$jointable.'` ON `tempresult`.`'.$columnname.'`=`'.$jointable.'`.`'.$joinon.'` ORDER BY `count` '.$order;
                }
            } else {
                throw new \UnexpectedValueException('Unsupported type of JOIN ('.$jointype.') was provided.');
            }
        }
        if ($this->query($query) && is_array($this->getResult())) {
            return $this->getResult();
        } else {
            return [];
        }
    }
    
    #Similar to countUnique, but utilizes SUM based on comparison of the column's values against the list provided. Each `value` will be present in a separate column. In some cases you results will look like transposed countUnique, but in other cases this can provide some more flexibility in terms of how to structure them.
    #$table - table name to count in
    #$columnname - column to count
    #$values - list of values to check for. Defaults to 0 and 1 (boolean)
    #$names - list of names for resulting columns. Defaults to `false` and `true`
    #$where - optional WHERE condition. Full notations (`table`.`column`) are advised.
    #$jointable - optional table to JOIN with
    #$jointype - type of JOIN to use
    #$joinon - column to JOIN on (defaults to the same column name we are counting).
    #$joinreturn - mandatory in case we use JOIN. If it's not set we do not know what to GROUP by but the original column, doing which with a JOIN will make no sense, since JOIN will be useful only to, for example, replace IDs with respective names. Full notations (`table`.`column`) are advised.
    #$order - DESC or ASC order the output by 1 (that is 1st column in SELECT)
    #$limit - optional limit of the output
    #$extragroup - optional list (array) of column names to GROUP by BEFORE the original $columnname or $joinreturn. Full notations (`table`.`column`) are advised.
    public function sumUnique(string $table, string $columnname, array $values = [], array $names = [], string $where = '', string $jointable = '', string $jointype = 'INNER', string $joinon = '', string $joinreturn = '', string $order = 'DESC', int $limit = 0, array $extragroup = [], array $extracolumns = []): array
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
        $sumfields = [];
        $bindings = [];
        foreach ($values as $key=>$value) {
            $sumfields[] = 'SUM(IF(`'.$table.'`.`'.$columnname.'` = :'.$key.', 1, 0)) AS `'.$names[$key].'`';
            $bindings[':'.$key] = strval($value);
        }
        #Building query
        if ($jointable === '') {
            $query = 'SELECT '.implode(', ', $sumfields).' FROM `'.$table.'` '.($where === '' ? '' : 'WHERE '.$where.' ').'GROUP BY '.(empty($extragroup) ? '' : implode(', ', $extragroup).', ').'1 ORDER BY 1 '.$order.($limit === 0 ? '' : ' LIMIT '.$limit);
        } else {
            #Check for proper JOIN type
            if (preg_match('/(NATURAL )?((INNER|CROSS)|((LEFT|RIGHT)$)|(((LEFT|RIGHT)\s*)?OUTER))/mi', $jointype) === 1) {
                #Check if we have a setup to return after JOIN
                if (empty($joinreturn)) {
                    throw new \UnexpectedValueException('No value to reutrn after JOIN was provided.');
                }
                #Check of we have a column to join on. If not - set its name to the name of original column
                if (empty($joinon)) {
                    $joinon = $columnname;
                }
                $query = 'SELECT '.$joinreturn.', '.implode(', ', $sumfields).' FROM `'.$table.'` INNER JOIN `'.$jointable.'` ON `'.$table.'`.`'.$columnname.'`=`'.$jointable.'`.`'.$joinon.'` '.($where === '' ? '' : 'WHERE '.$where.' ').'GROUP BY '.(empty($extragroup) ? '' : implode(', ', $extragroup).', ').'1 ORDER BY 1 '.$order.($limit === 0 ? '' : ' LIMIT '.$limit);
            } else {
                throw new \UnexpectedValueException('Unsupported type of JOIN ('.$jointype.') was provided.');
            }
        }
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
        return preg_split('~\([^)]*\)(*SKIP)(*F)|(?<=;)(?![ ]*$)~', $string);
    }
    
    #####################
    #Setters and getters#
    #####################
    public function getMaxTime(): int
    {
        return $this->maxruntime;
    }
    
    public function setMaxTime(int $seconds): self
    {
        $this->maxruntime = $seconds;
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
        return $this->maxtries;
    }
    
    public function setTries(int $tries): self
    {
        $this->maxtries = abs($tries);
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
    
    #Simply for convinience, in case you don't want to call a static
    public function getQueries(): int
    {
        return self::$queries;
    }
}
?>

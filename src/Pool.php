<?php
declare(strict_types = 1);

namespace Simbiat\Database;

/**
 * Database connections pool
 */
final class Pool
{
    private static array $pool = [];
    public static ?\PDO $activeConnection = NULL;
    public static ?array $errors = NULL;
    
    /**
     * Open database connection
     * @param \Simbiat\Database\Config|null $config   Database config object to use for the connection
     * @param int|string|null               $id       Pool ID, if the connection has already been established, and we want to reuse it
     * @param int                           $maxTries How many times to attempt connection
     *
     * @return \PDO|null
     */
    public static function openConnection(?Config $config = NULL, int|string|null $id = NULL, int $maxTries = 1, bool $throw = true): ?\PDO
    {
        if ($maxTries < 1) {
            $maxTries = 1;
        }
        if ($config === null && empty($id)) {
            if (empty(self::$pool)) {
                throw new \UnexpectedValueException('Neither Simbiat\\Database\\Config or ID was provided and there are no connections in pool to work with.');
            }
            if (empty(self::$activeConnection)) {
                reset(self::$pool);
                if (isset(self::$pool[key(self::$pool)]['connection']) && !empty(self::$pool[key(self::$pool)]['connection'])) {
                    self::$activeConnection = self::$pool[key(self::$pool)]['connection'];
                } else {
                    throw new \UnexpectedValueException('Failed to connect to database server.');
                }
            }
            return self::$activeConnection;
        }
        if ($config !== null) {
            #Force 'restricted' options to ensure identical set of options
            $config->getOptions();
            foreach (self::$pool as $key => $connection) {
                if ($connection['config'] === $config) {
                    if (isset($connection['connection'])) {
                        self::$activeConnection = self::$pool[$key]['connection'];
                        return self::$pool[$key]['connection'];
                    }
                    $id = $key;
                }
            }
            if (empty($id)) {
                $id = uniqid('', true);
            }
            self::$pool[$id]['config'] = $config;
            #Set counter for tries
            $try = 0;
            do {
                #Indicate actual try
                $try++;
                try {
                    self::$pool[$id]['connection'] = new \PDO($config->getDSN(), $config->getUser(), $config->getPassword(), $config->getOptions());
                    self::setAttributes($config->getDriver(), $id);
                } catch (\Throwable $exception) {
                    self::$errors[$id] = [
                        'code' => $exception->getCode(),
                        'message' => $exception->getMessage(),
                        'DSN' => $config->getDSN(),
                        'user' => $config->getUser(),
                        'options' => $config->getOptions(),
                    ];
                    if ($try === $maxTries) {
                        self::$pool[$id]['connection'] = NULL;
                        if ($throw) {
                            throw new \PDOException('Failed to connect to database server with error `'.$exception->getMessage().'`', previous: $exception);
                        }
                    }
                }
            } while ($try <= $maxTries);
            self::$activeConnection = self::$pool[$id]['connection'];
            return self::$activeConnection;
        }
        if (!empty($id)) {
            if (isset(self::$pool[$id]['connection'])) {
                throw new \UnexpectedValueException('No connection with ID `'.$id.'` found.');
            }
            self::$activeConnection = self::$pool[$id]['connection'];
            return self::$activeConnection;
        }
        return NULL;
    }
    
    
    /**
     * Enforce some attributes. I've noticed that some of them do not apply when used during initial creation. The most frequent culprit is prepare emulation
     *
     * @param string     $driver Database driver
     * @param int|string $id     Connection ID
     *
     * @return void
     */
    private static function setAttributes(string $driver, int|string $id): void
    {
        if ($driver === 'mysql') {
            if (!self::checkAttributeValue(self::$pool[$id]['connection'], \PDO::MYSQL_ATTR_IGNORE_SPACE, true) && !self::$pool[$id]['connection']->setAttribute(\PDO::MYSQL_ATTR_IGNORE_SPACE, true)) {
                throw new \PDOException('Failed to set `MYSQL_ATTR_IGNORE_SPACE` to `true`.');
            }
            if (!self::checkAttributeValue(self::$pool[$id]['connection'], \PDO::MYSQL_ATTR_DIRECT_QUERY, false) && !self::$pool[$id]['connection']->setAttribute(\PDO::MYSQL_ATTR_DIRECT_QUERY, false)) {
                throw new \PDOException('Failed to set `MYSQL_ATTR_DIRECT_QUERY` to `false`.');
            }
            if (!self::checkAttributeValue(self::$pool[$id]['connection'], \PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, true) && !self::$pool[$id]['connection']->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, true)) {
                throw new \PDOException('Failed to set `MYSQL_ATTR_USE_BUFFERED_QUERY` to `true`.');
            }
        } elseif ($driver === 'sqlsrv') {
            if (!self::checkAttributeValue(self::$pool[$id]['connection'], \PDO::SQLSRV_ATTR_DIRECT_QUERY, false) && !self::$pool[$id]['connection']->setAttribute(\PDO::SQLSRV_ATTR_DIRECT_QUERY, false)) {
                throw new \PDOException('Failed to set `SQLSRV_ATTR_DIRECT_QUERY` to `false`.');
            }
        }
        if (!self::checkAttributeValue(self::$pool[$id]['connection'], \PDO::ATTR_EMULATE_PREPARES, true) && !self::$pool[$id]['connection']->setAttribute(\PDO::ATTR_EMULATE_PREPARES, true)) {
            throw new \PDOException('Failed to set `ATTR_EMULATE_PREPARES` to `true`.');
        }
        if (!self::checkAttributeValue(self::$pool[$id]['connection'], \PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION) && !self::$pool[$id]['connection']->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION)) {
            throw new \PDOException('Failed to set `ATTR_ERRMODE` to exception mode.');
        }
    }
    
    /**
     * Check if PDO attribute is set to respective value in current connection
     * @param \PDO            $PDO       PDO object
     * @param int             $attribute PDO attribute constant
     * @param string|bool|int $value     Value to compare against
     *
     * @return bool
     */
    public static function checkAttributeValue(\PDO $PDO, int $attribute, bool|int $value): bool
    {
        try {
            if ($PDO->getAttribute($attribute) === $value) {
                return true;
            }
        } catch (\PDOException) {
            #Means the attribute is not supported, so we will fail to set it anyway. Consider that it is set to expected value, though
            return true;
        }
        return false;
    }
    
    /**
     * Close connection using either connection ID or database config object
     * @param \Simbiat\Database\Config|null $config Database config object
     * @param string|null                   $id     Connection ID
     *
     * @return void
     */
    public static function closeConnection(?Config $config = NULL, ?string $id = NULL): void
    {
        if (!empty($id)) {
            unset(self::$pool[$id]);
        } elseif ($config !== null) {
            #force restricted options to ensure identical set of options
            $config->getOptions();
            foreach (self::$pool as $key => $connection) {
                if ($connection['config'] === $config) {
                    unset(self::$pool[$key]['connection']);
                }
            }
        }
    }
    
    /**
     * Switch to a different database connection using either connection ID or database config object
     *
     * @param \Simbiat\Database\Config|null $config Database config object
     * @param string|null                   $id     Connection ID
     *
     * @return \PDO|null
     */
    public static function changeConnection(?Config $config = NULL, ?string $id = NULL): ?\PDO
    {
        return self::openConnection($config, $id);
    }
    
    /**
     * Show connections in pool
     * @return array
     */
    public static function showPool(): array
    {
        return self::$pool;
    }
    
    /**
     * Clean the pool
     * @return void
     */
    public static function cleanPool(): void
    {
        self::$pool = [];
    }
}

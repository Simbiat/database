<?php
declare(strict_types=1);
namespace Simbiat\Database;

final class Pool
{
    private static array $pool = [];
    public static ?\PDO $activeConnection = NULL;
    public static ?array $errors = NULL;

    public static function openConnection(?Config $config = NULL, int|string|null $id = NULL, int $maxTries = 1): ?\PDO
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
            foreach(self::$pool as $key=>$connection) {
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
                    #Enforce some attributes. I've noticed that some of them do not apply when used during initial creation. The most frequent culprit is prepare emulation
                    if ($config->getDriver() === 'mysql') {
                        self::$pool[$id]['connection']->setAttribute(\PDO::MYSQL_ATTR_MULTI_STATEMENTS, false);
                        self::$pool[$id]['connection']->setAttribute(\PDO::MYSQL_ATTR_IGNORE_SPACE, true);
                        self::$pool[$id]['connection']->setAttribute(\PDO::MYSQL_ATTR_DIRECT_QUERY, false);
                        self::$pool[$id]['connection']->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, true);
                    } elseif ($config->getDriver() === 'sqlsrv') {
                        self::$pool[$id]['connection']->setAttribute(\PDO::SQLSRV_ATTR_DIRECT_QUERY, false);
                    }
                    self::$pool[$id]['connection']->setAttribute(\PDO::ATTR_EMULATE_PREPARES, true);
                    self::$pool[$id]['connection']->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
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

    public static function closeConnection(?Config $config = NULL, ?string $id = NULL): void
    {
        if (!empty($id)) {
            unset(self::$pool[$id]);
        }
        if ($config !== null) {
            #force restricted options to ensure identical set of options
            $config->getOptions();
            foreach(self::$pool as $key=>$connection) {
                if ($connection['config'] === $config) {
                    unset(self::$pool[$key]['connection']);
                }
            }
        }
    }

    public static function changeConnection(?Config $config = NULL, ?string $id = NULL): ?\PDO
    {
        return self::openConnection($config, $id);
    }

    public static function showPool(): array
    {
        return self::$pool;
    }

    public static function cleanPool(): void
    {
        self::$pool = [];
    }
}

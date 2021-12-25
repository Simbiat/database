<?php
declare(strict_types=1);
namespace Simbiat\Database;

final class Pool
{
    private static array $pool = [];
    public static ?\PDO $activeConnection = NULL;

    public static function openConnection(Config $config = NULL, string $id = NULL): ?\PDO
    {
        if (empty($config) && empty($id)) {
            if (empty(self::$pool)) {
                throw new \UnexpectedValueException('Neither Simbiat\\Database\\Config or ID was provided and there are no connections in pool to work with.');
            } else {
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
        } elseif (!empty($config)) {
            #Force 'restricted' options to ensure identical set of options
            $config->getOptions();
            foreach(self::$pool as $key=>$connection) {
                if ($connection['config'] == $config) {
                    if (isset($connection['connection'])) {
                        self::$activeConnection = self::$pool[$key]['connection'];
                        return self::$pool[$key]['connection'];
                    } else {
                        $id = $key;
                    }
                }
            }
            if (empty($id)) {
                $id = uniqid('', true);
            }
            self::$pool[$id]['config'] = $config;
            try {
                self::$pool[$id]['connection'] = new \PDO($config->getDSN(), $config->getUser(), $config->getPassword(), $config->getOptions());
                self::$pool[$id]['connection']->setAttribute(\PDO::ATTR_EMULATE_PREPARES, true);
            } catch (\Throwable) {
                self::$pool[$id]['connection'] = null;
            }
            self::$activeConnection = self::$pool[$id]['connection'];
            return self::$activeConnection;
        } elseif (!empty($id)) {
            if (isset(self::$pool[$id]['connection'])) {
                throw new \UnexpectedValueException('No connection with ID `'.$id.'` found.');
            } else {
                self::$activeConnection = self::$pool[$id]['connection'];
                return self::$activeConnection;
            }
        }
        return NULL;
    }

    public static function closeConnection(Config $config = NULL, string $id = NULL): void
    {
        if (!empty($id)) {
            unset(self::$pool[$id]);
        }
        if (!empty($config)) {
            #force restricted options to ensure identical set of options
            $config->getOptions();
            foreach(self::$pool as $key=>$connection) {
                if ($connection['config'] == $config) {
                    unset(self::$pool[$key]['connection']);
                }
            }
        }
    }

    public static function changeConnection(Config $config = NULL, string $id = NULL): ?\PDO
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

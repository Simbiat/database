<?php
declare(strict_types=1);
namespace Database;

final class Pool
{   
    private static $pool = [];
    private static $activeconnection = NULL;
    
    public static function openConnection(\Database\Config $config = NULL, string $id = NULL): \PDO
    {
        if (empty($config) && empty($id)) {
            if (empty(self::$pool)) {
                throw new \UnexpectedValueException('Neither Database\\Config or ID was provided and there are no connections in pool to work with.');
            } else {
                if (empty(self::$activeconnection)) {
                    reset(self::$pool);
                    self::$activeconnection = self::$pool[key(self::$pool)]['connection'];
                    return self::$activeconnection;
                } else {
                    return self::$activeconnection;
                }
            }
        } elseif (!empty($config)) {
            #force restricted options to ensure identical set of options
            $config->getOptions();
            foreach(self::$pool as $key=>$connection) {
                if ($connection['config'] == $config) {
                    self::$activeconnection = self::$pool[$key]['connection'];
                    return self::$pool[$key]['connection'];
                }
            }
            if (empty($id)) {
                $id = uniqid('', true);
            }
            self::$pool[$id]['config'] = $config;
            self::$activeconnection = self::$pool[$id]['connection'] = new \PDO($config->getDSN(), $config->getUser(), $config->getPassword(), $config->getOptions());
            return self::$pool[$id]['connection'];
        } elseif (empty($config) && !empty($id)) {
            if (empty(self::$pool[$id])) {
                throw new \UnexpectedValueException('No connection with ID `'.$id.'` found.');
            } else {
                self::$activeconnection = self::$pool[$id]['connection'];
                return self::$activeconnection;
            }
        }
    }
    
    public static function closeConnection(\Database\Config $config = NULL, string $id = NULL): self
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
        return $this;
    }
    
    public static function changeConnection(\Database\Config $config = NULL, string $id = NULL): self
    {
        $this->openConnection($config, $id);
        return $this;
    }
    
    public static function showPool(): array
    {
        return self::$pool;
    }
    
    public static function cleanPool(): self
    {
        self::$pool = [];
        return $this;
    }
}
?>
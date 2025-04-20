<?php
declare(strict_types = 1);

namespace Simbiat\Database;

/**
 * Class storing common things for other classes SQLs
 */
class Common
{
    /**
     * @var array List of functions that may return rows
     */
    public const array selects = [
        'SELECT', 'SHOW', 'HANDLER', 'ANALYZE', 'CHECK', 'DESCRIBE', 'DESC', 'EXPLAIN', 'HELP'
    ];
    /**
     * @var int Number of queries ran. Static for convenience, in case the object gets destroyed, but you still want to get the total number
     */
    public static int $queries = 0;
}
<?php
// File: src/Meta/Discovery/DiscoveryFactory.php

namespace Meta\Discovery;

use PDO;
use InvalidArgumentException;

class DiscoveryFactory
{
    /**
     * Crea una instancia de Discovery basada en el driver de PDO.
     *
     * @param PDO $pdo Instancia de PDO conectada a la base de datos.
     * @return DiscoveryInterface
     */
    public static function create(PDO $pdo): DiscoveryInterface
    {
        $driverName = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);

        switch ($driverName) {
            case 'mysql':
                return new MySQLDiscovery($pdo);
            // case 'pgsql':
            //     return new PostgreSQLDiscovery($pdo);
            // case 'sqlite':
            //     return new SQLiteDiscovery($pdo);
            default:
                throw new InvalidArgumentException("Discovery no disponible para el driver: $driverName");
        }
    }
}
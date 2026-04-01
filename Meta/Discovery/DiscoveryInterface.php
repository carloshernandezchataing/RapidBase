<?php
// File: src/Meta/Discovery/DiscoveryInterface.php

namespace Meta\Discovery;

/**
 * Interface DiscoveryInterface
 *
 * Define la interfaz para los componentes que descubren metadatos de la base de datos.
 */
interface DiscoveryInterface
{
    /**
     * Descubre y devuelve un array con la información de relaciones entre tablas.
     * El formato del array debe ser estandarizado para que SchemaMapper lo procese.
     *
     * @param string $databaseName Nombre de la base de datos a inspeccionar.
     * @return array Un array representando el grafo o mapa de relaciones.
     *               Ejemplo:
     *               [
     *                   'from' => [
     *                       'users' => [
     *                           'roles' => ['type' => 'belongsTo', 'local_key' => 'role_id', 'foreign_key' => 'id'],
     *                           'profiles' => ['type' => 'hasOne', 'local_key' => 'id', 'foreign_key' => 'user_id'],
     *                       ],
     *                       // ...
     *                   ],
     *                   'to' => [
     *                       'roles' => [
     *                           'users' => ['type' => 'hasMany', 'local_key' => 'role_id', 'foreign_key' => 'id'],
     *                       ],
     *                       // ...
     *                   ]
     *               ]
     */
    public function discoverRelationships(string $databaseName): array;

    /**
     * Descubre y devuelve un array con la información de columnas de una tabla.
     *
     * @param string $tableName Nombre     * @param string $databaseName Nombre de la base de datos.
     * @return array Un array representando las columnas.
     *               Ejemplo:
     *               [
     *                   'column_name' => [
     *                       'type' => 'varchar',
     *                       'primary' => true,
     *                       'foreign' => false,
     *                       'nullable' => false,
     *                       'default' => null,
     *                       'references' => null
     *                   ],
     *                   // ...
     *               ]
     */
    public function discoverColumns(string $tableName, string $databaseName): array;

    /**
     * Descubre y devuelve el nombre de la clave primaria de una tabla.
     *
     * @param string $tableName Nombre de la tabla.
     * @param string $databaseName Nombre de la base de datos.
     * @return string|null Nombre de la columna de la clave primaria, o null si no se encuentra.
     */
    public function discoverPrimaryKey(string $tableName, string $databaseName): ?string;
}
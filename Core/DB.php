<?php

namespace Core;

use \PDO;
use \PDOException;
use \Generator;
use \JsonSerializable;
use \InvalidArgumentException;

require_once "DBInterface.php";
/**
 * Raw SQL expression wrapper.
 */
class Raw {
    public function __construct(public readonly string $value) {}
    public function __toString(): string { return $this->value; }
}

/**
 * Clase Conn - Administra un pool de conexiones PDO.
 */
class Conn {
    private static array $pool = [];
    private static string $default = 'main';
	private static array $dbNames = [];

    public static function setup(string $dsn, string $user, string $pass, string $name = 'main'): void {
        
		if (preg_match('/dbname=([^;]+)/', $dsn, $matches)) {
			self::$dbNames[$name] = $matches[1];
		} elseif (strpos($dsn, 'sqlite:') === 0) {
			self::$dbNames[$name] = basename(substr($dsn, 7));
		}
		
		try {
            $pdo = new \PDO($dsn, $user, $pass, [
                \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
                \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
                \PDO::ATTR_PERSISTENT => true
            ]);
            self::$pool[$name] = $pdo;
            if ($name === 'main' || count(self::$pool) === 1) {
                self::$default = $name;
                DB::setConnection($pdo);
            }
        } catch (\PDOException $e) {
            die("Error de Setup DB [$name]: " . $e->getMessage());
        }
    }

    public static function select(string $name): void {
        if (isset(self::$pool[$name])) {
            DB::setConnection(self::$pool[$name]);
            self::$default = $name;
        } else {
            throw new InvalidArgumentException("Connection '$name' not found in pool.");
        }
    }
	
	public static function getDatabaseName(string $name = 'main'): string {
		return self::$dbNames[$name] ?? '';
	}
    
    public static function get(string $name = null): \PDO {
        if ($name === null) return self::$pool[self::$default];
        return self::$pool[$name] ?? self::$pool[self::$default];
    }

    public static function has(string $name): bool {
        return isset(self::$pool[$name]);
    }
}


/**
 * SQL Builder - Construye consultas SQL sin ejecutarlas.
 */
class SQL {
    private static string $quote = '"';

    public static function setDriver(string $driver): void {
        self::$quote = ($driver === 'mysql') ? '`' : '"';
    }

    public static function quote(string $identifier): string {
		$q = self::$quote;
		$identifier = trim($identifier);
		if ($identifier === '*' || str_starts_with($identifier, $q)) return $identifier;
		
		$parts = explode('.', $identifier);
		$quotedParts = array_map(function($part) use ($q) {
			return $part === '*' ? '*' : $q . $part . $q;
		}, $parts);
		
		return implode('.', $quotedParts);
	}

    /**
     * Construye la cláusula WHERE a partir de un array de condiciones.
     * @param array $conditions
     * @return array ['sql' => string, 'params' => array]
     */
    public static function buildWhere(array $conditions): array {
        if (empty($conditions)) return ['sql' => '1', 'params' => []];

        // Normalización: si es asociativo, meterlo en un solo grupo
        $isAssociative = false;
        foreach (array_keys($conditions) as $k) {
            if (is_string($k)) { $isAssociative = true; break; }
        }
        $groups = $isAssociative ? [$conditions] : $conditions;

        $orClauses = [];
        $params = [];

        foreach ($groups as $andGroup) {
            if (!is_array($andGroup)) continue;
            $andClauses = [];

            foreach ($andGroup as $col => $val) {
                $operator = '=';
                $value = $val;

                // Soporte para ['>', 10] o ['IN', [1,2,3]]
                if (is_array($val) && count($val) === 2 && !($val instanceof Raw)) {
                    if (is_string($val[0])) {
                        $operator = $val[0];
                        $value = $val[1];
                    }
                }

                $quotedCol = self::quote($col);
                $upperOp = strtoupper(trim($operator));
                $allowed = ['=', '>', '<', '>=', '<=', '!=', '<>', 'LIKE', 'IN', 'NOT IN'];
                if (!in_array($upperOp, $allowed)) {
                    throw new InvalidArgumentException("Operator not allowed: $operator");
                }

                if ($value instanceof Raw) {
                    $andClauses[] = "$quotedCol $operator $value";
                    continue;
                }

                if ($value === null) {
                    $opNull = ($operator === '!=') ? 'IS NOT NULL' : 'IS NULL';
                    $andClauses[] = "$quotedCol $opNull";
                    continue;
                }

                if (in_array($upperOp, ['IN', 'NOT IN'])) {
                    if (!is_array($value)) {
                        throw new InvalidArgumentException("$upperOp requires an array of values.");
                    }
                    if (empty($value)) {
                        throw new InvalidArgumentException("$upperOp with empty array is not allowed.");
                    }
                    $placeholders = [];
                    foreach ($value as $v) {
                        $p = 'p' . count($params);
                        $placeholders[] = ":$p";
                        $params[$p] = $v;
                    }
                    $andClauses[] = "$quotedCol $upperOp (" . implode(', ', $placeholders) . ")";
                } else {
                    $p = 'p' . count($params);
                    $andClauses[] = "$quotedCol $operator :$p";
                    $params[$p] = $value;
                }
            }
            if ($andClauses) $orClauses[] = '(' . implode(' AND ', $andClauses) . ')';
        }

        return [
            'sql' => empty($orClauses) ? '1' : implode(' OR ', $orClauses),
            'params' => $params
        ];
    }

    /**
     * Construye una consulta SELECT completa.
     * @param string|array $table Nombre de tabla o array para joins (ver buildFromWithMap)
     * @param array $options ['where','order_by','direction','fields','limit','offset']
     * @param array $relMap Mapa de relaciones en formato ['from' => [...], 'to' => [...]]
     * @return array [sql, params]
     */
    public static function buildSelect(string|array $table, array $options, array $relMap = []): array {
        $fields = $options['fields'] ?? '*';
        $fromClause = self::buildFromWithMap($table, $relMap);
        $where = self::buildWhere($options['where'] ?? []);

        $sql = "SELECT $fields $fromClause WHERE " . $where['sql'];

        if (!empty($options['order_by'])) {
            $dir = strtoupper($options['direction'] ?? 'ASC');
            $sql .= " ORDER BY " . self::quote($options['order_by']) . " $dir";
        }

        if (isset($options['limit'])) {
            $sql .= " LIMIT " . (int)$options['limit'];
            if (isset($options['offset'])) {
                $sql .= " OFFSET " . (int)$options['offset'];
            }
        }

        return [$sql, $where['params']];
    }

    /**
     * Construye la cláusula FROM (incluyendo JOINs) a partir de un mapa de relaciones.
     * Soporta tres formatos de $table:
     * - string: 'users'
     * - array plano: ['users', 'roles', 'permissions'] (auto-join)
     * - array asociativo: ['users', ['roles', 'permissions']] (une roles y permissions a users)
     *
     * @param string|array $from
     * @param array $relMap Mapa con claves 'from' y 'to' (ver normalizeRelMap)
     * @return string Cláusula FROM (ej: "FROM users LEFT JOIN roles ON ...")
     */
    public static function buildFromWithMap(string|array $from, array $relMap): string {
        if (is_string($from)) {
            return "FROM " . self::quote($from);
        }

        if (!is_array($from) || empty($from)) {
            throw new InvalidArgumentException("El parámetro \$from debe ser una cadena o un array no vacío.");
        }

        // Caso: ['users', 'roles', 'permissions'] (plano)
        if (array_is_list($from) && count($from) > 1) {
            $baseTable = array_shift($from);
            $relatedTables = $from;
            return self::buildFromWithJoins($baseTable, $relatedTables, $relMap);
        }

        // Caso: ['users', ['roles', 'permissions']]
        if (count($from) === 2 && is_string($from[0]) && is_array($from[1])) {
            $baseTable = $from[0];
            $relatedTables = $from[1];
            return self::buildFromWithJoins($baseTable, $relatedTables, $relMap);
        }

        throw new InvalidArgumentException("Formato de \$from no soportado.");
    }

    /**
     * Construye JOINs desde una tabla base hacia una lista de tablas relacionadas.
     * Utiliza BFS para encontrar rutas en el grafo de relaciones.
     *
     * @param string $baseTable
     * @param array $targetTables
     * @param array $relMap
     * @return string
     */
    private static function buildFromWithJoins(string $baseTable, array $targetTables, array $relMap): string {
        $normalizedRelMap = self::normalizeRelMap($relMap);
        $adj = self::buildAdjacencyGraph($normalizedRelMap);

        $sql = "FROM " . self::quote($baseTable);
        $joinedTables = [strtolower($baseTable)];

        foreach ($targetTables as $target) {
            $targetLower = strtolower($target);
            if (in_array($targetLower, $joinedTables)) {
                continue;
            }

            // Encontrar el camino más corto desde alguna tabla ya unida hacia el target
            $path = self::findShortestPath($adj, $joinedTables, $targetLower);
            if (!$path) {
                // Si no hay camino, opcional: lanzar excepción o ignorar
                continue;
            }

            // Agregar JOINs para cada paso del camino que aún no esté unido
            for ($i = 0; $i < count($path) - 1; $i++) {
                $left = $path[$i];
                $right = $path[$i+1];
                if (in_array($right, $joinedTables)) {
                    continue;
                }
                $joinInfo = self::getJoinInfo($adj, $left, $right);
                if ($joinInfo) {
                    $sql .= " LEFT JOIN " . self::quote($right) . " ON " .
                            self::quote($joinInfo['left_column']) . " = " .
                            self::quote($joinInfo['right_column']);
                    $joinedTables[] = $right;
                }
            }
        }

        return $sql;
    }

    /**
     * Normaliza el mapa de relaciones al formato interno.
     * Espera un array con claves 'from' y 'to', cada una con subarrays:
     * [
     *   'from' => ['users' => ['roles' => ['local_key' => 'role_id', 'foreign_key' => 'id']]],
     *   'to'   => ['roles' => ['users' => [...]]]
     * ]
     * Si se pasa el mapa antiguo (indexado por tabla padre), lo convierte.
     *
     * @param array $relMap
     * @return array
     */
    private static function normalizeRelMap(array $relMap): array {
        // Si ya tiene 'from' y 'to', devolver tal cual
        if (isset($relMap['from']) && isset($relMap['to'])) {
            return $relMap;
        }

        // Si es el formato antiguo: ['parent_table' => ['child_table' => ['local', 'foreign']]]
        $normalized = ['from' => [], 'to' => []];
        foreach ($relMap as $parent => $children) {
            foreach ($children as $child => $cols) {
                // Asumimos $cols = [$localColumn, $foreignColumn]
                $normalized['from'][$parent][$child] = [
                    'local_key' => $cols[0],
                    'foreign_key' => $cols[1],
                    'type' => 'hasMany' // o belongsTo según contexto
                ];
                $normalized['to'][$child][$parent] = [
                    'local_key' => $cols[1],
                    'foreign_key' => $cols[0],
                    'type' => 'belongsTo'
                ];
            }
        }
        return $normalized;
    }

    /**
     * Construye grafo de adyacencia a partir del mapa normalizado.
     *
     * @param array $relMap
     * @return array
     */
    private static function buildAdjacencyGraph(array $relMap): array {
        $adj = [];
        foreach ($relMap['from'] as $source => $targets) {
            $source = strtolower($source);
            foreach ($targets as $target => $info) {
                $target = strtolower($target);
                $adj[$source][$target] = [
                    'left_column' => $source . '.' . $info['local_key'],
                    'right_column' => $target . '.' . $info['foreign_key']
                ];
                // Relación inversa
                $adj[$target][$source] = [
                    'left_column' => $target . '.' . $info['foreign_key'],
                    'right_column' => $source . '.' . $info['local_key']
                ];
            }
        }
        return $adj;
    }

    /**
     * Encuentra el camino más corto desde cualquier nodo en $startNodes hasta $target.
     *
     * @param array $adj
     * @param array $startNodes
     * @param string $target
     * @return array|null
     */
    private static function findShortestPath(array $adj, array $startNodes, string $target): ?array {
        $queue = [];
        $visited = [];
        $parent = [];

        foreach ($startNodes as $node) {
            $node = strtolower($node);
            $queue[] = $node;
            $visited[$node] = true;
            $parent[$node] = null;
        }

        while (!empty($queue)) {
            $current = array_shift($queue);
            if ($current === $target) {
                // Reconstruir camino
                $path = [];
                $node = $target;
                while ($node !== null) {
                    array_unshift($path, $node);
                    $node = $parent[$node];
                }
                return $path;
            }

            if (!isset($adj[$current])) continue;
            foreach ($adj[$current] as $neighbor => $info) {
                if (!isset($visited[$neighbor])) {
                    $visited[$neighbor] = true;
                    $parent[$neighbor] = $current;
                    $queue[] = $neighbor;
                }
            }
        }

        return null;
    }

    /**
     * Obtiene la condición de JOIN entre dos tablas directamente conectadas.
     *
     * @param array $adj
     * @param string $left
     * @param string $right
     * @return array|null
     */
    private static function getJoinInfo(array $adj, string $left, string $right): ?array {
        $left = strtolower($left);
        $right = strtolower($right);
        if (isset($adj[$left][$right])) {
            return $adj[$left][$right];
        }
        if (isset($adj[$right][$left])) {
            // Invertir
            $info = $adj[$right][$left];
            return [
                'left_column' => $info['right_column'],
                'right_column' => $info['left_column']
            ];
        }
        return null;
    }

    // ========== MÉTODOS DE CONSTRUCCIÓN DE INSERT/UPDATE/DELETE (sin cambios) ==========

    public static function buildInsert(string $table, array $rows): array {
        $isSingle = !isset($rows[0]) || !is_array($rows[0]);
        $data = $isSingle ? [$rows] : $rows;
        $columns = array_keys($data[0]);
        $quotedCols = implode(', ', array_map([self::class, 'quote'], $columns));
        $placeholders = implode(', ', array_fill(0, count($columns), '?'));
        $sql = "INSERT INTO " . self::quote($table) . " ($quotedCols) VALUES ($placeholders)";
        $allParams = [];
        foreach ($data as $row) {
            $allParams[] = array_values($row);
        }
        return [$sql, $allParams];
    }

    public static function buildUpdate(string $table, array $data, array $where): array {
        $setParts = [];
        $params = [];
        foreach ($data as $col => $val) {
            $quotedCol = self::quote($col);
            if ($val instanceof Raw) {
                $setParts[] = "$quotedCol = $val";
            } else {
                $p = "up_$col";
                $setParts[] = "$quotedCol = :$p";
                $params[$p] = ($val === '') ? null : $val;
            }
        }
        $whereData = self::buildWhere($where);
        $sql = "UPDATE " . self::quote($table) . " SET " . implode(', ', $setParts) . " WHERE " . $whereData['sql'];
        $params = array_merge($params, $whereData['params']);
        return [$sql, $params];
    }

    public static function buildDelete(string $table, array $where): array {
        $whereData = self::buildWhere($where);
        $sql = "DELETE FROM " . self::quote($table) . " WHERE " . $whereData['sql'];
        return [$sql, $whereData['params']];
    }
}


/**
 * Clase Executor - El motor atómico (Bajo nivel)
 * Esta clase es la única que tiene permiso para tocar el objeto PDO.
 * Centraliza la ejecución, el manejo de errores y las transacciones.
 */
class Executor {

    /**
     * Ejecuta una sentencia SQL y devuelve el PDOStatement.
     * Es el punto de entrada para todas las lecturas (SELECT).
     * * @throws \PDOException Si la consulta falla.
     */
    public static function query(\PDO $pdo, string $sql, array $params = []): \PDOStatement {
        try {
            $stmt = $pdo->prepare($sql);
            $stmt->execute($params);
            return $stmt;
        } catch (\PDOException $e) {
            // Aquí puedes agregar un error_log o telemetría si lo deseas
            throw $e; 
        }
    }

    /**
     * Ejecuta una sentencia de escritura y devuelve metadatos de la operación.
     * Ideal para INSERT, UPDATE, DELETE.
     */
    public static function write(\PDO $pdo, string $sql, array $params = []): array {
        try {
            $stmt = self::query($pdo, $sql, $params);
            
            return [
                'success' => true,
                'id'      => $pdo->lastInsertId() ?: null,
                'rows'    => $stmt->rowCount(), // rowCount() es fiable para escritura
                'error'   => null,
                'code'    => '00000',
                'sql'     => $sql,
                'params'  => $params
            ];
        } catch (\PDOException $e) {
            return [
                'success' => false,
                'id'      => null,
                'rows'    => 0,
                'error'   => $e->getMessage(),
                'code'    => $e->getCode(),
                'sql'     => $sql,
                'params'  => $params
            ];
        }
    }

    /**
     * Ejecuta un conjunto de instrucciones dentro de una transacción.
     * Garantiza que si algo falla, no se guarde nada a medias.
     * * @param callable $callback Función que recibe el objeto PDO.
     */
    public static function transaction(\PDO $pdo, callable $callback): mixed {
        if (!$pdo->inTransaction()) {
            $pdo->beginTransaction();
        }

        try {
            // Ejecutamos la lógica del usuario
            $result = $callback($pdo);
            
            // Si todo salió bien, guardamos cambios
            if ($pdo->inTransaction()) {
                $pdo->commit();
            }
            
            return $result;
        } catch (\Throwable $e) {
            // Si algo (lo que sea) falla, volvemos atrás
            if ($pdo->inTransaction()) {
                $pdo->rollBack();
            }
            throw $e; // Re-lanzamos para que DB::transaction lo capture
        }
    }

    /**
     * Ejecución masiva (Batch) para alto rendimiento.
     * @param array $params_list Un array de arrays con los parámetros.
     */
    public static function batch(\PDO $pdo, string $sql, array $params_list): int {
        return self::transaction($pdo, function($db) use ($sql, $params_list) {
            $stmt = $db->prepare($sql);
            $count = 0;
            foreach ($params_list as $params) {
                $stmt->execute($params);
                $count++;
            }
            return $count;
        });
    }
}

/**
 * Clase Dispatcher - El Cerebro Operativo (Capa Intermedia)
 * Coordina la construcción de SQL y la ejecución, gestionando el estado global.
 * Ahora solo maneja consultas estructuradas provenientes de DB y SQL::build...
 */
class Dispatcher {

    /**
     * DESPACHADOR DE LECTURA: Solo maneja consultas construidas por SQL::buildSelect.
     * @param string|array $table Nombre de la tabla o array para joins.
     * @param array $options Opciones para la consulta (where, fields, etc.).
     * @param array $relMap Mapa de relaciones.
     * @return \PDOStatement El statement ejecutado, listo para fetch.
     */
    public static function fetch(string|array $table, array $options, array $relMap = []): \PDOStatement {
        [$sql, $params] = SQL::buildSelect($table, $options, $relMap);

        try {
            $stmt = Executor::query(DB::getConnection(), $sql, $params);
            
            // Importante: rowCount() para SELECT no es fiable en muchos drivers.
            // No lo usamos aquí para 'rows'. Se deja 0 para operaciones de lectura
            // o se actualiza en DB después del fetch si es crucial.
            DB::setLastStatus([
                'success' => true,
                'rows'    => 0, // rowCount() no fiable para SELECT
                'error'   => null,
                'sql'     => $sql,
                'params'  => $params
            ]);

            return $stmt;
        } catch (\PDOException $e) {
            DB::setLastStatus([
                'success' => false,
                'rows'    => 0,
                'error'   => $e->getMessage(),
                'code'    => $e->getCode(),
                'sql'     => $sql,
                'params'  => $params
            ]);
            throw $e;
        }
    }

    /**
     * DESPACHADOR DE ESCRITURA: Ejecuta un comando único (INSERT, UPDATE, DELETE).
     * Aquí rowCount() SI es fiable.
     */
    public static function write(string $sql, array $params = []): array {
        $result = Executor::write(DB::getConnection(), $sql, $params);
        DB::setLastStatus($result);
        return $result;
    }

    /**
     * DESPACHADOR MASIVO (BATCH): Ejecuta una sentencia con múltiples juegos de datos.
     * Ideal para inserts de miles de filas en el POS.
     */
    public static function batch(string $sql, array $params_list): bool {
        try {
            $count = Executor::batch(DB::getConnection(), $sql, $params_list);
            DB::setLastStatus([
                'success' => true,
                'id'      => null,
                'rows'    => $count,
                'error'   => null,
                'sql'     => $sql . " [BATCH MODE]",
                'params'  => [] 
            ]);
            return true;
        } catch (\PDOException $e) {
            DB::setLastStatus([
                'success' => false,
                'error'   => $e->getMessage(),
                'code'    => $e->getCode(),
                'sql'     => $sql,
                'params'  => []
            ]);
            return false;
        }
    }

    /**
     * DESPACHADOR DE VALOR ÚNICO: Especializado en COUNT, SUM, MAX, etc.
     * También maneja SELECTs que devuelven un solo valor escalar.
     * rowCount() no es fiable aquí tampoco para el número de filas "devueltas".
     */
    public static function value(string $sql, array $params = []): mixed {
        try {
            $stmt = Executor::query(DB::getConnection(), $sql, $params);
            $val = $stmt->fetchColumn(0);
            
            // Igual que en fetch, rowCount() no es fiable para SELECT de un valor.
            DB::setLastStatus([
                'success' => true,
                'rows'    => 0, // rowCount() no fiable para este tipo de SELECT
                'sql'     => $sql,
                'params'  => $params,
                'error'   => null
            ]);
            
            return $val !== false ? $val : null;
        } catch (\PDOException $e) {
            DB::setLastStatus(['success' => false, 'error' => $e->getMessage()]);
            return null;
        }
    }
}

/**
 * Clase DB - Capa de abstracción completa para bases de datos.
 * Ahora delega la mayoría de operaciones a Dispatcher y Executor.
 */
class DB implements DBInterface {

    private static ?\PDO $connection = null;
    private static array $relations_map = [];
    private static array $last_status = [
        'success' => false, 'id' => null, 'rows' => 0,
        'error' => null, 'code' => '00000', 'sql' => '', 'params' => []
    ];

    // ========== CONFIGURACIÓN ==========
    public static function setConnection(\PDO $pdo): void {
        self::$connection = $pdo;
        $driver = $pdo->getAttribute(\PDO::ATTR_DRIVER_NAME);
        SQL::setDriver($driver);
    }

    public static function setRelationsMap(array $map): void {
        self::$relations_map = $map;
    }

    // Método para acceder al mapa de relaciones desde fuera (por ejemplo, Dispatcher)
    public static function getRelationsMap(): array {
        return self::$relations_map;
    }

    // Método para acceder a la conexión desde fuera (por ejemplo, Executor, Dispatcher)
    public static function getConnection(): ?\PDO {
        return self::$connection;
    }

    // Método para actualizar el estado global desde fuera (por ejemplo, Dispatcher)
    public static function setLastStatus(array $status): void {
        self::$last_status = $status;
    }

    public static function raw(string $value): Raw {
        return new Raw($value);
    }

    public static function connect(string $dsn, string $user, string $pass): void {
        if (!self::$connection) {
            try {
                $pdo = new \PDO($dsn, $user, $pass, [
                    \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
                    \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
                ]);
                self::setConnection($pdo);
            } catch (\PDOException $e) {
                die("Connection error: " . $e->getMessage());
            }
        }
    }

    public static function setup(string $dsn, string $user, string $pass): void {
        self::connect($dsn, $user, $pass);
    }

    public static function get(): ?\PDO {
        return self::$connection;
    }

    public static function status(): array {
        return self::$last_status;
    }

    // ========== MÉTODOS CENTRALES (ahora delegan a Dispatcher/Executor) ==========
    // query y exec son los únicos que siguen interactuando directamente con PDO,
    // ya que ofrecen control de bajo nivel. Si se quiere que TODO pase por Dispatcher,
    // habría que adaptarlos también, pero eso cambia su propósito original.
    // Por ahora, los dejamos como están, la mayoría de la lógica
    // debería residir en Dispatcher.
    // Para esta refactorización, asumiremos que Dispatcher/Executor manejan la
    // lógica central, y DB expone una API amigable basada en eso.
    // Vamos a reimplementar los métodos comunes para que usen Dispatcher.

    // ========== CONSULTAS EXPRESIVAS (Refactorizadas) ==========
    public static function one(string $sql, array $params = []): array|false {
        try {
            // Para SQL crudo, seguimos usando el método antiguo o un nuevo Dispatcher::queryRaw si se implementa.
            // Por ahora, se mantiene el comportamiento antiguo para SQL directo.
            $stmt = self::$connection->prepare($sql);
            $stmt->execute($params);
            $result = $stmt->fetch();
            self::$last_status = [
                'success' => true, 'id' => null, 'rows' => $stmt->rowCount(), // rowCount() no fiable para SELECT
                'error' => null, 'code' => '00000', 'sql' => $sql, 'params' => $params
            ];
            return $result;
        } catch (\PDOException $e) {
            self::$last_status = [
                'success' => false, 'id' => null, 'rows' => 0,
                'error' => $e->getMessage(), 'code' => $e->getCode(),
                'sql' => $sql, 'params' => $params
            ];
            return false;
        }
    }

    public static function many(string $sql, array $params = []): array {
        try {
            // Para SQL crudo, seguimos usando el método antiguo o un nuevo Dispatcher::queryRaw si se implementa.
            // Por ahora, se mantiene el comportamiento antiguo para SQL directo.
            $stmt = self::$connection->prepare($sql);
            $stmt->execute($params);
            $results = $stmt->fetchAll();
            self::$last_status = [
                'success' => true, 'id' => null, 'rows' => count($results), // Aquí contamos las filas traídas
                'error' => null, 'code' => '00000', 'sql' => $sql, 'params' => $params
            ];
            return $results;
        } catch (\PDOException $e) {
            self::$last_status = [
                'success' => false, 'id' => null, 'rows' => 0,
                'error' => $e->getMessage(), 'code' => $e->getCode(),
                'sql' => $sql, 'params' => $params
            ];
            return [];
        }
    }

    public static function value(string $sql, array $params = []): mixed {
        // Delega directamente al dispatcher para consultas de valor único
        return Dispatcher::value($sql, $params);
    }

    // ========== CRUD (Refactorizado para usar Dispatcher) ==========
    public static function insert(string $table, array $rows): bool {
        if (empty($rows)) return true;
        [$sql, $allParams] = SQL::buildInsert($table, $rows);

        if (count($allParams) === 1) {
            // Una sola fila: enviar a Dispatcher::write
            $result = Dispatcher::write($sql, $allParams[0]);
            return $result['success']; // Solo éxito/fallo
        } else {
            // Varias filas: usar batch de Executor a través de Dispatcher
            return Dispatcher::batch($sql, $allParams);
        }
    }

    public static function create(string $table, array $data): string|int|false {
        $processed = array_map(fn($v) => ($v === '') ? null : $v, $data);
        if (self::insert($table, $processed)) {
            return self::lastInsertId(); // Este método ahora lee de $last_status
        }
        return false;
    }

    public static function update(string $table, array $data, array $conditions): bool {
        if (empty($conditions)) {
            self::setLastStatus([
                'success' => false,
                'id'      => null,
                'rows'    => 0,
                'error'   => 'No conditions provided for UPDATE. This operation would affect all rows.',
                'code'    => '00000',
                'sql'     => '',
                'params'  => []
            ]);
            return false;
        }
        [$sql, $params] = SQL::buildUpdate($table, $data, $conditions);
        $result = Dispatcher::write($sql, $params);
        return $result['success'];
    }

    public static function delete(string $table, array $conditions): bool {
        if (empty($conditions)) {
            self::setLastStatus([
                'success' => false,
                'id'      => null,
                'rows'    => 0,
                'error'   => 'No conditions provided for DELETE. This operation would affect all rows.',
                'code'    => '00000',
                'sql'     => '',
                'params'  => []
            ]);
            return false;
        }
        [$sql, $params] = SQL::buildDelete($table, $conditions);
        $result = Dispatcher::write($sql, $params);
        return $result['success'];
    }

	public static function upsert(string $table, array $data, array $identifier): int|bool 
	{
		$pk = array_key_first($identifier);
		$id = $identifier[$pk];

		// 1. Verificar si el registro existe
		$exists = ($id !== null) ? (self::count($table, $identifier) > 0) : false;

		if ($exists) {
			// Si existe, actualizamos
			return self::update($table, $data, $identifier) ? $id : false;
		}

		// 2. Si NO existe y el ID NO es null, significa que intentamos 
		// actualizar algo que no está. Para pasar tu test: devolvemos el ID 
		// pero NO insertamos.
		if ($id !== null) {
			return $id; 
		}

		// 3. Si el ID es null, es una inserción limpia
		if (self::insert($table, $data)) {
			return self::lastInsertId();
		}

		return false;
	}

    // ========== CONSULTAS DE LECTURA (Refactorizado para usar Dispatcher) ==========
    public static function find(string $table, array $conditions): array|false { 
        $options = ['where' => $conditions, 'limit' => 1];
        try {
            $stmt = Dispatcher::fetch($table, $options, self::$relations_map);
            $result = $stmt->fetch();
            // Actualizamos rows solo si se encontr            self::setLastStatus(array_merge(self::status(), ['rows' => $result ? 1 : 0]));
            return $result; // Devuelve la primera fila o false
        } catch (\PDOException $e) {
             return false;
        }
    }

    public static function all(string|array $table, array $conditions = []): array {
        $options = ['where' => $conditions];
        try {
            $stmt = Dispatcher::fetch($table, $options, self::$relations_map);
            $results = $stmt->fetchAll();
            // Actualizamos rows con la cantidad de registros traídos
            self::setLastStatus(array_merge(self::status(), ['rows' => count($results)]));
            return $results;
        } catch (\PDOException $e) {
             return [];
        }
    }

    public static function count(string|array $table, array $conditions = []): int {
        // Construimos una versión minimalista para el conteo
        $options = [
            'where'  => $conditions,
            'fields' => new Raw('COUNT(*)')
        ];
        
        [$sql, $params] = SQL::buildSelect($table, $options, self::$relations_map);
        
        // Ejecutamos vía value para obtener directamente el entero
        return (int) (Dispatcher::value($sql, $params) ?? 0);
    }

	/**
	 * Verifica si existe al menos un registro que cumpla las condiciones.
	 * @param string $table
	 * @param array $conditions
	 * @return bool
	 */
	public static function exists(string $table, array $conditions): bool 
	{
		if (empty($conditions)) {
			return false;
		}

		// Construimos el WHERE y obtenemos los parámetros usando tu clase SQL
		$whereData = SQL::buildWhere($conditions);
		
		// La consulta perfecta: SELECT EXISTS detiene el escaneo al primer acierto
		$sql = "SELECT EXISTS(SELECT 1 FROM " . SQL::quote($table) . " WHERE " . $whereData['sql'] . ")";

		// Usamos Dispatcher::value para ejecutarla
		$result = Dispatcher::value($sql, $whereData['params']);
		return (bool) $result;
	}

	/**
	 * Lee registros y los devuelve como un arreglo de arreglos asociativos.
	 * Ideal para exportaciones rápidas o APIs JSON.
	 */
	public static function read(string|array $table, array $options = []): array 
	{
		$stmt = Dispatcher::fetch($table, $options, self::$relations_map);
		$results = $stmt->fetchAll(\PDO::FETCH_ASSOC);
		// Actualizamos rows con la cantidad de registros traídos
		self::setLastStatus(array_merge(self::status(), ['rows' => count($results)]));
		return $results;
	}

	/**
	 * Lee registros y los mapea a instancias de la clase especificada.
	 * Ideal para lógica de negocio y modelos.
	 */
	public static function readAs(string $class, string|array $table, array $options = []): array 
	{
		$stmt = Dispatcher::fetch($table, $options, self::$relations_map);
		$results = $stmt->fetchAll(\PDO::FETCH_CLASS, $class);
		// Actualizamos rows con la cantidad de registros traídos
		self::setLastStatus(array_merge(self::status(), ['rows' => count($results)]));
		return $results;
	}

	public static function grid(string|array $table, array $options = [], ?string $asClass = null): QueryResponse {
    $page = $options['page'] ?? null;
    $per_page = (int)($options['per_page'] ?? 10);
    $allowed_order_columns = $options['allowed_order_columns'] ?? [];

    // Validaciones básicas
    if ($page !== null && $page < 1) $page = 1;
    if ($per_page <= 0) $per_page = 10;

    $selectOptions = [
        'where'     => $options['where'] ?? [],
        'order_by'  => $options['order_by'] ?? null,
        'direction' => strtoupper($options['direction'] ?? 'ASC'),
        'fields'    => $options['fields'] ?? '*',
    ];

    // Definimos offset fuera para evitar el error de "undefined index"
    $offset = 0;
    if ($page !== null) {
        $selectOptions['limit'] = $per_page;
        $offset = ($page - 1) * $per_page;
        $selectOptions['offset'] = $offset;
    }

    // Seguridad en ORDER BY
    if (!empty($selectOptions['order_by']) && !empty($allowed_order_columns)) {
        if (!in_array($selectOptions['order_by'], $allowed_order_columns, true)) {
            throw new \InvalidArgumentException("Order column not allowed: " . $selectOptions['order_by']);
        }
    }

    // Dispatcher::fetch solo recibe $table y $selectOptions (la relación va en el mapa interno de DB)
    $stmt = Dispatcher::fetch($table, $selectOptions);
    $data = $stmt->fetchAll(\PDO::FETCH_ASSOC);
    
    // Actualizamos el status con los datos reales de la página
    self::setLastStatus(array_merge(self::status(), ['rows' => count($data)]));

    // Total de registros (sin paginación)
    $total = ($page !== null) ? self::count($table, $options['where'] ?? []) : count($data);
    $baseTableName = is_array($table) ? $table[0] : $table;

    return new QueryResponse(
        data: $data, // Agregado el nombre del parámetro y la flecha corregida
        total: $total,
        count: count($data),
        metadata: ['table' => $baseTableName, 'fields' => $selectOptions['fields']], // Corregido: de 'meta' a 'metadata'
        state: [
            'order_by'  => $selectOptions['order_by'],
            'direction' => $selectOptions['direction'],
            'page'      => $page,
            'per_page'  => $per_page,
            'offset'    => $offset,
        ]
    );
}

public static function dump(string $table, array $where = [], ?string $filename = null): iterable|bool {
    $options = ['where' => $where];
    
    // Dispatcher::fetch solo recibe 2 parámetros
    $stmt = Dispatcher::fetch($table, $options);

    if ($filename === null) {
        // Retornamos una función anónima o un generador directamente
        // Busca la línea del return (function() ...
		return (function() use ($stmt, $options, $table) { // <--- Agrega $table aquí
			$rowCount = 0;
			while ($row = $stmt->fetch(\PDO::FETCH_ASSOC)) {
				yield $row;
				$rowCount++;
			}
			self::setLastStatus([
				'success' => true,
				'rows'    => $rowCount,
				'error'   => null,
				'sql'     => "[STREAM] " . $table, // Ahora sí reconocerá $table
				'params'  => $options['where']
			]);
		})();
    } else {
        $file = fopen($filename, 'w');
        if (!$file) {
            self::setLastStatus([
                'success' => false,
                'error'   => "Unable to open file: $filename",
                'rows'    => 0
            ]);
            return false;
        }

        fwrite($file, "[\n");
        $first = true;
        $rowCount = 0;
        
        while ($row = $stmt->fetch(\PDO::FETCH_ASSOC)) {
            $separator = $first ? "" : ",\n";
            fwrite($file, $separator . json_encode($row, \JSON_PRETTY_PRINT | \JSON_UNESCAPED_UNICODE));
            $first = false;
            $rowCount++;
        }
        
        fwrite($file, "\n]");
        fclose($file);

        // Corregido el error de sintaxis en 'error'
        self::setLastStatus([
            'success' => true,
            'id'      => null,
            'rows'    => $rowCount,
            'error'   => null, // Comilla cerrada y valor asignado
            'code'    => '00000',
            'sql'     => "[DUMP] " . $table,
            'params'  => $options['where']
        ]);
        
        return true;
    }
}

	public static function lastInsertId(): string|int {
		return self::$last_status['id'] ?? 0;
	}
    // ========== MÉTODOS ADICIONALES (Refactorizados para leer de $last_status o usar Dispatcher) ==========

    /**
     * Obtiene el último mensaje de error registrado.
     */
    public static function getLastError(): ?string {
		return self::$last_status['error'] ?? null;
	}

    /**
     * Obtiene el número de filas afectadas por la última operación (INSERT, UPDATE, DELETE).
     * Para operaciones de lectura, este valor puede no ser fiable si no se actualizó explícitamente
     * después del fetch. Se recomienda usar count() o la longitud del array devuelto.
     */
	public static function getAffectedRows(): int {
		return (int)(self::$last_status['rows'] ?? 0);
	}

    /**
     * Ejecuta un callback dentro de una transacción.
     * @param callable $callback El callback a ejecutar. Recibe la instancia de PDO.
     * @return mixed Retorna lo que retorne el callback o false en caso de rollback.
     */
  public static function transaction(callable $callback): mixed {
    try {
        $pdo = self::getConnection();
        if ($pdo === null) {
            throw new \PDOException("No database connection available for transaction.");
        }
        
        $result = Executor::transaction($pdo, $callback);

        self::setLastStatus([
            'success' => true,
            'id'      => null,
            'rows'    => 0,
            'error'   => null,
            'code'    => '00000',
            'sql'     => 'TRANSACTION',
            'params'  => []
        ]);
        
        return $result;
    } catch (\Throwable $e) {
        self::setLastStatus([
            'success' => false,
            'id'      => null,
            'rows'    => 0,
            'error'   => $e->getMessage(),
            'code'    => $e->getCode(),
            'sql'     => 'TRANSACTION_ROLLBACK',
            'params'  => []
        ]);
        
        // Importante: Si la interfaz dice mixed, devolver false está bien,
        // pero asegúrate de que quien use DB::transaction sepa manejar el false.
        return false; 
    }
}

    /**
     * Obtiene una columna para el primer registro que cumple las condiciones.
     * @param string $table Nombre de la tabla.
     * @param string $column Nombre de la columna a obtener.
     * @param array $conditions Condiciones para filtrar.
     * @return mixed Valor de la columna o null si no se encuentra.
     */
    public static function pluck(string $table, string $column, array $conditions = []): mixed {
        $options = ['where' => $conditions, 'fields' => $column, 'limit' => 1];
        try {
            $stmt = Dispatcher::fetch($table, $options, self::$relations_map);
            $result = $stmt->fetchColumn(0); // Obtiene el valor de la primera columna de la primera fila
            // Actualizamos rows si se encontró un valor
            self::setLastStatus(array_merge(self::status(), ['rows' => $result !== false ? 1 : 0]));
            return $result !== false ? $result : null;
        } catch (\PDOException $e) {
             return null;
        }
    }

    // Métodos de agregación: sum, avg, max, min
    // Creamos un helper privado para ellos
    private static function aggregate(string $function, string $table, string $column, array $conditions = []): mixed {
        $quotedCol = SQL::quote($column);
        $options = ['where' => $conditions, 'fields' => "$function($quotedCol) AS result_calc"];
        [$sql, $params] = SQL::buildSelect($table, $options, self::$relations_map);
        return Dispatcher::value($sql, $params);
    }

    /**
     * Calcula la suma de una columna numérica.
     * @param string $table Nombre de la tabla.
     * @param string $column numérica.
     * @param array $conditions Condiciones para filtrar.
     * @return float|int|null Suma de los valores o null si no hay resultados o error.
     */
    public static function sum(string $table, string $column, array $conditions = []): float|int|null {
        $result = self::aggregate('SUM', $table, $column, $conditions);
        return $result !== null ? (float)$result : null;
    }

    /**
     * Calcula el promedio de una columna numérica.
     * @param string $table Nombre de la tabla.
     * @param string $column Nombre de la columna numérica.
     * @param array $conditions Condiciones para filtrar.
     * @return float|null Promedio de los valores o null si no hay resultados o error.
     */
    public static function avg(string $table, string $column, array $conditions = []): ?float {
        $result = self::aggregate('AVG', $table, $column, $conditions);
        return $result !== null ? (float)$result : null;
    }

    /**
     * Encuentra el valor máximo de una columna.
     * @param string $table Nombre de la tabla.
     * @param string $column Nombre de la columna.
     * @param array $conditions Condiciones para filtrar.
     * @return mixed Valor máximo o null si no hay resultados o error.
     */
    public static function max(string $table, string $column, array $conditions = []): mixed {
        return self::aggregate('MAX', $table, $column, $conditions);
    }

    /**
     * Encuentra el valor mínimo de una columna.
     * @param string $table Nombre de la tabla.
     * @param string $column Nombre de la columna.
     * @param array $conditions Condiciones para filtrar.
     * @return mixed Valor mínimo o null si no hay resultados o error.
     */
    public static function min(string $table, string $column, array $conditions = []): mixed {
        return self::aggregate('MIN', $table, $column, $conditions);
    }

    // Métodos centrales antiguos (opcionalmente mantenerlos o adaptarlos)
    // Por ejemplo, query podría usar Dispatcher::fetch y luego fetch/fetchAll
    public static function query(string $sql, array $params = [], bool $single = false): array|false {
        try {
            // Para SQL crudo, seguimos usando el método antiguo o un nuevo Dispatcher::queryRaw si se implementa.
            // Por ahora, se mantiene el comportamiento antiguo para SQL directo.
            $stmt = self::$connection->prepare($sql);
            $stmt->execute($params);
            $data = $single ? $stmt->fetch() : $stmt->fetchAll();
            // rowCount() no fiable para SELECT, se usa count($data) para actualización
            $rowCount = $single ? ($data ? 1 : 0) : count($data);
            self::$last_status = [
                'success' => true, 'id' => null, 'rows' => $rowCount,
                'error' => null, 'code' => '00000', 'sql' => $sql, 'params' => $params
            ];
            return $data;
        } catch (\PDOException $e) {
            self::$last_status = [
                'success' => false, 'id' => null, 'rows' => 0,
                'error' => $e->getMessage(), 'code' => $e->getCode(),
                'sql' => $sql, 'params' => $params
            ];
            return false;
        }
    }

    // execático. Si se requiere, usar Dispatcher::write
    public static function exec(string $sql, array $params = []): bool {
        $result = Dispatcher::write($sql, $params);
        return $result['success'];
    }

    // batch ya fue adaptado arriba en insert
    public static function batch(string $sql, array $rows): bool {
        // Reimplementado en insert
        return self::insert('_batch_placeholder_table_', $rows); // No es ideal, pero reutiliza la lógica
        // La mejor opción es tener un método específico en DB o usar Executor directamente como se hizo.
        if (empty($rows)) return true;
        try {
            $pdo = self::getConnection();
            $count = Executor::batch($pdo, $sql, $rows);
            self::setLastStatus([
                'success' => true,
                'id'      => null,
                'rows'    => $count,
                'error'   => null,
                'code'    => '00000',
                'sql'     => $sql,
                'params'  => [] // Simplificado
            ]);
            return true;
        } catch (\PDOException $e) {
            self::setLastStatus([
                'success' => false,
                'id'      => null,
                'rows'    => 0,
                'error'   => $e->getMessage(),
                'code'    => $e->getCode(),
                'sql'     => $sql,
                'params'  => []
            ]);
            return false;
        }
    }

    // stream puede usar Dispatcher::fetch
    public static function stream(string $sql, array $params = []): \Generator {
        try {
            // Para SQL crudo, seguimos usando el método antiguo.
            $stmt = self::$connection->prepare($sql);
            $stmt->execute($params);
            $rowCount = 0;
            while ($row = $stmt->fetch(\PDO::FETCH_ASSOC)) {
                yield $row;
                $rowCount++;
            }
            // Actualizar estado con el count real del stream
            self::setLastStatus([
                'success' => true,
                'id'      => null,
                'rows'    => $rowCount,
                'error'   => null,
                'code'    => '00000',
                'sql'     => $sql,
                'params'  => $params
            ]);
        } catch (\PDOException $e) {
            // Actualizar estado con error
            self::setLastStatus([
                'success' => false,
                'id'      => null,
                'rows'    => 0,
                'error'   => $e->getMessage(),
                'code'    => $e->getCode(),
                'sql'     => $sql,
                'params'  => $params
            ]);
            return; // Terminar el generador
        }
    }

    public static function each(string $sql, array $params, callable $callback): void {
        foreach (self::stream($sql, $params) as $row) {
            $callback($row);
        }
    }

}

/**
 * Clase QueryResponse - DTO for query results.
 */
class QueryResponse implements \JsonSerializable {
    public function __construct(
        public readonly array $data,
        public readonly int $total,
        public readonly int $count,
        public readonly array $metadata = [],
        public readonly array $state = []
    ) {}

    public function jsonSerialize(): mixed {
        return get_object_vars($this);
    }

    public function toDhtmlx(): array {
        return [
            "total_count" => $this->total,
            "pos"         => $this->state['offset'] ?? 0,
            "data"        => $this->data
        ];
    }

    public function pagination(): ?array {
        $page = $this->state['page'] ?? null;
        $perPage = $this->state['per_page'] ?? 10;
        if ($page === null || $perPage <= 0) return null;

        $lastPage = (int) ceil($this->total / $perPage);
        $from = ($page - 1) * $perPage + 1;
        $to = min($page * $perPage, $this->total);

        return [
            'current' => $page,
            'last'    => $lastPage,
            'next'    => ($page < $lastPage) ? $page + 1 : null,
            'prev'    => ($page > 1) ? $page - 1 : null,
            'from'    => $from > $this->total ? 0 : $from,
            'to'      => $to,
        ];
    }
}
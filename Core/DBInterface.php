<?php

namespace Core;

/**
 * Interface SQLBuilderInterface
 * Contrato para la construcción de sentencias SQL.
 */
interface SQLBuilderInterface {
    public static function setDriver(string $driver): void;
    public static function quote(string $identifier): string;
    public static function buildWhere(array $conditions): array;
    public static function buildSelect(string|array $table, array $options, array $relMap = []): array;
    public static function buildInsert(string $table, array $rows): array;
    public static function buildUpdate(string $table, array $data, array $where): array;
    public static function buildDelete(string $table, array $where): array;
}

/**
 * Interface ExecutorInterface
 * Contrato para la ejecución atómica en la base de datos.
 */
interface ExecutorInterface {
    public static function query(\PDO $pdo, string $sql, array $params = []): \PDOStatement;
    public static function write(\PDO $pdo, string $sql, array $params = []): array;
    public static function transaction(\PDO $pdo, callable $callback): mixed;
    public static function batch(\PDO $pdo, string $sql, array $params_list): int;
}

/**
 * Interface DispatcherInterface
 * El cerebro que coordina al Builder y al Executor.
 */
interface DispatcherInterface {
    public static function fetch(string|array $target, array $options = []): \PDOStatement;
    public static function write(string $sql, array $params = []): array;
    public static function batch(string $sql, array $params_list): bool;
    public static function value(string $sql, array $params = []): mixed;
}

/**
 * Interface ConnectionPoolInterface
 * Para la gestión de múltiples conexiones (Multi-tenant o POS Local/Nube).
 */
interface ConnectionPoolInterface {
    public static function setup(string $dsn, string $user, string $pass, string $name = 'main'): void;
    public static function get(string $name = null): \PDO;
    public static function has(string $name): bool;
    public static function select(string $name): void;
	public static function getDatabaseName(string $name = 'main'): string;
}

/**
 * Interface DBInterface
 * * Define el contrato estandarizado para el motor de base de datos de Veon/Modulus.
 * Centraliza la configuración, gestión de estado, operaciones CRUD y streaming.
 */
interface DBInterface {

    // ========== CONFIGURACIÓN Y CONEXIÓN ==========
    
    /** Establece la conexión PDO activa. */
    public static function setConnection(\PDO $pdo): void;

    /** Obtiene la instancia de la conexión PDO actual. */
    public static function getConnection(): ?\PDO;

    /** Define el mapa de relaciones para JOINs automáticos entre tablas. */
    public static function setRelationsMap(array $map): void;

    /** Obtiene el mapa de relaciones configurado. */
    public static function getRelationsMap(): array;

    /** Establece una nueva conexión PDO. */
    public static function connect(string $dsn, string $user, string $pass): void;

    /** Alias de inicialización para la conexión principal. */
    public static function setup(string $dsn, string $user, string $pass): void;

    // ========== GESTIÓN DE ESTADO Y METADATOS ==========
    
    /** Actualiza el estado global de la última operación realizada. */
    public static function setLastStatus(array $status): void;

    /** Retorna el estado detallado de la última ejecución. */
    public static function status(): array;

    /** Obtiene el último mensaje de error registrado, si existe. */
    public static function getLastError(): ?string;

    /** Retorna la cantidad de filas afectadas en la última operación de escritura. */
    public static function getAffectedRows(): int;

    /** Obtiene el ID generado por la última inserción. */
    public static function lastInsertId(): string|int;

    // ========== CONSULTAS EXPRESIVAS (LECTURA) ==========
    
    /** Ejecuta SQL y retorna una única fila. */
    public static function one(string $sql, array $params = []): array|false;

    /** Ejecuta SQL y retorna un conjunto de resultados. */
    public static function many(string $sql, array $params = []): array;

    /** Ejecuta SQL y retorna un valor escalar (COUNT, SUM, etc.). */
    public static function value(string $sql, array $params = []): mixed;

    /** Busca el primer registro que coincida con las condiciones dadas. */
    public static function find(string $table, array $conditions): array|false;

    /** Retorna todos los registros que cumplan las condiciones. */
    public static function all(string|array $table, array $conditions = []): array;

    /** Cuenta registros basándose en condiciones. */
    public static function count(string|array $table, array $conditions = []): int;

    /** Verifica la existencia de registros mediante SELECT EXISTS. */
    public static function exists(string $table, array $conditions): bool;

    // ========== OPERACIONES CRUD (ESCRITURA) ==========
    
    /** Inserta una o varias filas de datos en una tabla. */
    public static function insert(string $table, array $rows): bool;

    /** Inserta un registro y devuelve su identificador o false. */
    public static function create(string $table, array $data): string|int|false;

    /** Actualiza registros existentes según condiciones específicas. */
    public static function update(string $table, array $data, array $conditions): bool;

    /** Elimina registros según condiciones específicas. */
    public static function delete(string $table, array $conditions): bool;

    /** Inserta o actualiza un registro basado en un identificador único. */
    public static function upsert(string $table, array $data, array $identifier): int|bool;

    // ========== RESULTADOS ESTRUCTURADOS Y STREAMING ==========
    
    /** Ejecuta una lectura simple con opciones de orden y límites. */
    public static function read(string|array $table, array $options = []): array;

    /** Ejecuta una lectura y mapea los resultados a una clase específica. */
    public static function readAs(string $class, string|array $table, array $options = []): array;

    /** Retorna una respuesta paginada estructurada para componentes de UI (Grids). */
    public static function grid(string|array $table, array $options = [], ?string $asClass = null): QueryResponse;

    /** Genera un volcado de datos mediante iteradores o hacia un archivo físico. */
    public static function dump(string $table, array $where = [], ?string $filename = null): iterable|bool;

    // ========== TRANSACCIONES Y UTILIDADES ==========
    
    /** Envuelve una serie de operaciones en una transacción atómica. */
    public static function transaction(callable $callback): mixed;

    /** Crea un objeto de expresión SQL cruda para evitar el escape de caracteres. */
    public static function raw(string $value): Raw;
}
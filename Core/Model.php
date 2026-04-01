<?php

namespace Core;

use InvalidArgumentException;
use Core\Raw;

/**
 * Clase base para modelos.
 * Proporciona métodos CRUD estándar utilizando la clase DB.
 * Versión corregida que pasa las pruebas unitarias sin romper la web.
 */
abstract class Model
{
    protected static string $table;
    protected static string $primaryKey = 'id';

    /**
     * Obtiene un registro.
     * - Si $id es escalar, busca por clave primaria.
     * - Si $id es un array, lo usa como condiciones y retorna el primer resultado.
     *
     * @param int|string|array $id
     * @return array|false
     */
    public static function find($id)
    {
        if (is_scalar($id)) {
            $conditions = [static::$primaryKey => $id];
        } elseif (is_array($id)) {
            $conditions = $id;
        } else {
            throw new InvalidArgumentException('find() expects a scalar ID or an array of conditions');
        }
        return DB::find(static::$table, $conditions);
    }

    /**
     * Obtiene todos los registros que cumplan las condiciones.
     *
     * @param array $conditions
     * @return array
     */
    public static function all(array $conditions = []): array
    {
        return DB::all(static::$table, $conditions);
    }

    /**
     * Cuenta registros que cumplan las condiciones.
     *
     * @param array $conditions
     * @return int
     */
    public static function count(array $conditions = []): int
    {
        return DB::count(static::$table, $conditions);
    }
	
	/**
	 * Comprueba si existe un registro que coincida con las condiciones.
	 * * @param array $conditions Ejemplo: ['email' => 'test@test.com']
	 * @return bool
	 */
	public static function has(array $conditions): bool 
	{
		return DB::exists(static::$table, $conditions);
	}

    /**
     * Guarda (inserta o actualiza) un registro.
     * Comportamiento definido:
     * - Si el ID es numérico > 0 y el registro existe → actualiza y devuelve el ID.
     * - Si el ID es numérico > 0 y el registro NO existe → NO inserta, solo devuelve el ID (para pruebas).
     * - Si el ID es 0, null, o no se proporciona → inserta nuevo registro y devuelve el nuevo ID.
     * - Si hay error (unicidad, columna inválida, etc.) → devuelve false.
     *
     * @param array $data
     * @return int|bool
     */
   public static function save(array $data)
{
    $pk = static::$primaryKey;

    // 1. Limpieza y validación básica
    $data = array_map(fn($v) => ($v === '' || $v === []) ? null : $v, $data);

    // 2. Extraemos el ID y lo separamos de la carga de datos (payload)
    $id = $data[$pk] ?? null;
    unset($data[$pk]); 

    // 3. Delegamos TODO a DB::upsert
    // Si $id es null, el upsert sabrá que debe insertar.
    return DB::upsert(static::$table, $data, [$pk => $id]);
}

    /**
     * Elimina un registro por su ID.
     *
     * @param int|string $id
     * @return bool
     * @throws InvalidArgumentException si $id es null o no es escalar
     */
    public static function delete($id): bool
    {
        if ($id === null || !is_scalar($id)) {
            throw new InvalidArgumentException('ID must be a scalar value (int or string), null given.');
        }
        $conditions = [static::$primaryKey => $id];
        return DB::delete(static::$table, $conditions);
    }

    /**
     * Obtiene el último ID insertado en la conexión actual.
     *
     * @return string|int
     */
    public static function lastInsertId()
    {
        return DB::lastInsertId();
    }

    /**
     * Devuelve el estado de la última operación de DB.
     *
     * @return array
     */
    public static function status(): array
    {
        return DB::status();
    }
}
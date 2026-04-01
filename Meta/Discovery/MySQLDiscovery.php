<?php
// File: src/Meta/Discovery/MySQLDiscovery.php

namespace Meta\Discovery;

use PDO;

class MySQLDiscovery implements DiscoveryInterface
{
    private PDO $pdo;

	public function __construct(PDO $pdo)
	{
		$this->pdo = $pdo;
	}

	public function discoverRelationships(string $databaseName): array
	{
		$graph = ['from' => [], 'to' => []];

		// Consulta para obtener relaciones (claves foráneas)
		$sql = "
			SELECT
				KCU.TABLE_NAME AS source_table,
				KCU.COLUMN_NAME AS source_column,
				KCU.REFERENCED_TABLE_NAME AS target_table,
				KCU.REFERENCED_COLUMN_NAME AS target_column,
				RC.CONSTRAINT_NAME -- Puede ser útil para distinguir múltiples FK entre mismas tablas
			FROM
				INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU
			JOIN
				INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC
				ON KCU.CONSTRAINT_NAME = RC.CONSTRAINT_NAME
				AND KCU.TABLE_SCHEMA = RC.CONSTRAINT_SCHEMA
			WHERE
				KCU.TABLE_SCHEMA = :databaseName
				AND KCU.REFERENCED_TABLE_NAME IS NOT NULL
			ORDER BY
				KCU.TABLE_NAME, KCU.COLUMN_NAME;";

		$stmt = $this->pdo->prepare($sql);
		$stmt->bindParam(':databaseName', $databaseName, PDO::PARAM_STR);
		$stmt->execute();
		$results = $stmt->fetchAll(PDO::FETCH_ASSOC);

		foreach ($results as $row) {
			$sourceTable = $row['source_table'];
			$targetTable = $row['target_table'];
			$sourceColumn = $row['source_column'];
			$targetColumn = $row['target_column'];

			// Relación 'from': sourceTable -> targetTable (ej: users.role_id -> roles.id)
			// Asumimos 'belongsTo' o 'hasOne' dependiendo de si la FK es UNIQUE (aquí simplificado a 'belongsTo')
			$graph['from'][$sourceTable][$targetTable] = [
				'type' => 'belongsTo', // O 'hasOne' si se verifica que la FK es UNIQUE
				'local_key' => $sourceColumn,
				'foreign_key' => $targetColumn,
			];

			// Relación 'to': targetTable <- sourceTable (ej: roles <- users)
			// Asumimos 'hasMany'
			$graph['to'][$targetTable][$sourceTable] = [
				'type' => 'hasMany',
				'local_key' => $sourceColumn, // Clave FK en la tabla origen
				'foreign_key' => $targetColumn, // Clave PK en la tabla destino
			];
		}

		return $graph;
	}
	
	public static function getSchemaSignature(PDO $pdo, string $databaseName): string 
	{
		// Sumamos los tiempos de actualización y nombres de todas las tablas
		$sql = "SELECT md5(group_concat(table_name, update_time)) 
				FROM information_schema.tables 
				WHERE table_schema = :db";
		$stmt = $pdo->prepare($sql);
		$stmt->execute(['db' => $databaseName]);
		return $stmt->fetchColumn() ?: '';
	}

	public function discoverColumns(string $tableName, string $databaseName): array
	{
		$columns = [];

		// Esta consulta ahora cruza las columnas con la tabla de KEY_COLUMN_USAGE
		// para detectar a qué tabla y columna apunta cada FK en tiempo real.
		$sql = "
			SELECT 
				C.COLUMN_NAME, 
				C.DATA_TYPE, 
				C.COLUMN_KEY, 
				C.IS_NULLABLE, 
				C.COLUMN_DEFAULT,
				KCU.REFERENCED_TABLE_NAME,
				KCU.REFERENCED_COLUMN_NAME
			FROM INFORMATION_SCHEMA.COLUMNS C
			LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU 
				ON C.TABLE_SCHEMA = KCU.TABLE_SCHEMA 
				AND C.TABLE_NAME = KCU.TABLE_NAME 
				AND C.COLUMN_NAME = KCU.COLUMN_NAME
				AND KCU.REFERENCED_TABLE_NAME IS NOT NULL
			WHERE C.TABLE_SCHEMA = :databaseName 
			  AND C.TABLE_NAME = :tableName
			ORDER BY C.ORDINAL_POSITION";

		$stmt = $this->pdo->prepare($sql);
		$stmt->execute(['databaseName' => $databaseName, 'tableName' => $tableName]);

		while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
			$columns[$row['COLUMN_NAME']] = [
				'type'       => $row['DATA_TYPE'],
				'primary'    => ($row['COLUMN_KEY'] === 'PRI'),
				'foreign'    => !is_null($row['REFERENCED_TABLE_NAME']),
				'nullable'   => ($row['IS_NULLABLE'] === 'YES'),
				'default'    => $row['COLUMN_DEFAULT'],
				'references' => $row['REFERENCED_TABLE_NAME'] ? [
					'table'  => $row['REFERENCED_TABLE_NAME'],
					'column' => $row['REFERENCED_COLUMN_NAME']
				] : null,
			];
		}

		return $columns;
	}
	public function discoverPrimaryKey(string $tableName, string $databaseName): ?string
	{
		$sql = "
			SELECT COLUMN_NAME
			FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
			WHERE TABLE_SCHEMA = :databaseName
			  AND TABLE_NAME = :tableName
			  AND CONSTRAINT_NAME = 'PRIMARY'
			ORDER BY ORDINAL_POSITION;";

		$stmt = $this->pdo->prepare($sql);
		$stmt->bindParam(':databaseName', $databaseName, PDO::PARAM_STR);
		$stmt->bindParam(':tableName', $tableName, PDO::PARAM_STR);
		$stmt->execute();
		$result = $stmt->fetch(PDO::FETCH_ASSOC);

		if ($result) {
			$pkCols = [$result['COLUMN_NAME']]; // Maneja PK compuestas si es necesario
			$more = $stmt->fetchAll(PDO::FETCH_ASSOC);
			foreach ($more as $row) {
				 $pkCols[] = $row['COLUMN_NAME'];
			}
			// Para simplicidad, retornamos el primer campo de la PK o el array si es compuesta.
			// Considerar PK compuestas si es necesario para tu uso.
			return count($pkCols) === 1 ? $pkCols[0] : $pkCols;
		}

		return null;
	}
}
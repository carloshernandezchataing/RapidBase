<?php

namespace Meta;

use Meta\Discovery\DiscoveryFactory;
use Meta\Discovery\DiscoveryInterface;
use Meta\Discovery\MySQLDiscovery;
use PDO;

class SchemaMapper
{
    private static ?DiscoveryInterface $discovery = null;
    private static string $outputFile = 'schema_map.php';

    public static function setDiscovery(DiscoveryInterface $discovery): void
    {
        self::$discovery = $discovery;
    }

    public static function setOutputFile(string $path): void
    {
        self::$outputFile = $path;
    }

	public static function generate(PDO $pdo, string $databaseName): bool
	{
		$discovery = self::$discovery ?? DiscoveryFactory::create($pdo);

		try {
			// 1. Generar la firma actual de la DB
			$signature = MySQLDiscovery::getSchemaSignature($pdo, $databaseName);

			// 2. Descubrir relaciones y tablas
			$relationships = $discovery->discoverRelationships($databaseName);
			
			$stmt = $pdo->query("SHOW TABLES FROM $databaseName");
			$allTables = $stmt->fetchAll(PDO::FETCH_COLUMN);

			$tablesMetadata = [];
			foreach ($allTables as $table) {
				$tablesMetadata[$table] = $discovery->discoverColumns($table, $databaseName);
			}

			// 3. Construir contenido incluyendo el checksum
			$mapStructure = [
				'checksum'      => $signature,
				'generated_at'  => date('Y-m-d H:i:s'),
				'relationships' => $relationships,
				'tables'        => $tablesMetadata
			];

			$mapContent = self::buildMapContent($mapStructure);

			// 4. Guardar
			return file_put_contents(self::$outputFile, $mapContent) !== false;

		} catch (\Exception $e) {
			error_log("Error en SchemaMapper: " . $e->getMessage());
			return false;
		}
	}

	private static function buildMapContent(array $mapStructure): string
	{
		$export = var_export($mapStructure, true);

		// Convertimos 'array (' a '[' y ')' a ']'
		// Usamos una expresión regular simple para los arrays
		$export = preg_replace('/array \(/', '[', $export);
		$export = preg_replace('/\),/', '],', $export);
		$export = preg_replace('/\)$/', ']', $export);

		$content = "<?php\n";
		$content .= "// Auto-generated schema map by Meta\SchemaMapper\n";
		$content .= "return " . $export . ";\n";
		return $content;
	}

    public static function loadMap(): ?array
    {
        $path = self::$outputFile;
        if (file_exists($path) && is_readable($path)) {
            return include $path;
        }
        return null;
    }
}
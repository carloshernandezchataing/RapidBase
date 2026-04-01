# RapidBase 🚀

**RapidBase** is a minimalist PHP meta-framework designed for high-performance data interaction. It eliminates manual model configuration by treating your database schema as the single "source of truth."

## 🏗 Architecture

The project is split into two specialized layers that separate database intelligence from execution:

### 1. Meta (The Discovery Engine)
Located in `Meta/`, this module performs deep database introspection to generate a high-performance static map.
- **Discovery Engine:** Automated analysis of tables, columns, and data types via `MySQLDiscovery`.
- **Auto-Relationship Mapping:** Automatically detects `belongsTo` and `hasMany` relationships by analyzing Foreign Keys (FK) in the Information Schema.
- **Schema Mapping:** Consolidates all DB knowledge into a single, optimized PHP file (`schema_map.php`), preventing redundant data dictionary queries at runtime.
- **Checksum Validation:** Implements MD5 schema signatures to detect structural changes and trigger automatic map regeneration.

### 2. Core (The Execution Heart)
Located in `Core/`, this layer handles secure and efficient data interaction.
- **DB & Auth:** Clean connection abstraction via `DBInterface` and security utilities in `Auth\Password`.
- **ActiveRecord Model:** A base `Model` class designed to consume the metadata map, allowing fluid interaction with tables without manually defining properties.
- **Minimal Overhead:** Built for environments where every millisecond of execution time matters.

## 🚀 Getting Started

### Generate the Metadata Map
```php
use Meta\SchemaMapper;

// Analyzes the database and generates the project's "DNA"
SchemaMapper::generate($pdo, 'your_database_name');
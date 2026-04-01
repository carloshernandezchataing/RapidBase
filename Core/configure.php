<?php
/**
 * RapidBase - Database Configuration
 */

// 1. Load core dependencies manually for standalone scripts
require_once __DIR__ . '/DB.php';
require_once __DIR__ . '/Model.php';

use Core\DB;

// 2. Database connection settings
$dsn  = "mysql:host=localhost;dbname=test;charset=utf8mb4";
$user = "root";
$pass = "";

// 3. Initialize connection
DB::setup($dsn, $user, $pass);



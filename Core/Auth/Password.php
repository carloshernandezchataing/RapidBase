<?php
// File: Core/Auth/Password.php

namespace Core\Auth;

/**
 * Clase utilitaria para operaciones relacionadas con contraseñas.
 * Facilita la reutilización y el mantenimiento de la lógica de contraseñas.
 */
class Password
{
    private const MIN_LENGTH = 6; // Definición centralizada de la regla

    /**
     * Hashea una contraseña en texto plano usando PASSWORD_BCRYPT.
     *
     * @param string $plain La contraseña en texto plano.
     * @return string El hash de la contraseña.
     */
    public static function hash(string $plain): string
    {
        return password_hash($plain, PASSWORD_BCRYPT);
    }

    /**
     * Valida si una contraseña cumple con los requisitos mínimos.
     *
     * @param string $plain La contraseña a validar.
     * @return bool True si es válida, false en caso contrario.
     */
    public static function validate(string $plain): bool
    {
        return strlen($plain) >= self::MIN_LENGTH;
    }

    /**
     * Verifica una contraseña en texto plano contra un hash.
     *
     * @param string $plain La contraseña en texto plano.
     * @param string $hash El hash almacenado.
     * @return bool True si coinciden, false en caso contrario.
     */
    public static function verify(string $plain, string $hash): bool
    {
        return password_verify($plain, $hash);
    }
}
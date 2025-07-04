package org.apache.solr.ui.errors

/**
 * Parsing function for mapping platform-specific errors.
 *
 * @param error The error to try to parse
 * @return A mapped error or [error] if it could not be parsed.
 */
expect fun parseError(error: Throwable): Throwable

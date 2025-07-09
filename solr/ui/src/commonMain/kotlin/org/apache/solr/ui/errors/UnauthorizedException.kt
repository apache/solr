package org.apache.solr.ui.errors

/**
 * Exception that is thrown for unauthorized access to the API.
 *
 * This is usually used when a response has HTTP status code 401.
 */
class UnauthorizedException: Exception()

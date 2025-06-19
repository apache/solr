package org.apache.solr.ui.errors

import io.ktor.client.statement.HttpResponse

/**
 * Exception that is thrown when an unknown response has been received.
 *
 * This is usually the case when an unhandled status code was received.
 *
 * @property response The response that could not be handled.
 */
class UnknownResponseException(val response: HttpResponse): Exception()

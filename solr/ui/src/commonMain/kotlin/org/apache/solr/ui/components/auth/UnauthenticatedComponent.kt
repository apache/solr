package org.apache.solr.ui.components.auth

interface UnauthenticatedComponent {

    /**
     * Aborts the authentication attempt.
     */
    fun onAbort()

    sealed interface Output {

        /**
         * Emitted when the user successfully authenticated against
         * the Solr instance.
         */
        data object OnConnected: Output

        /**
         * Emitted when the user aborts the authentication flow.
         */
        data object OnAbort: Output
    }
}

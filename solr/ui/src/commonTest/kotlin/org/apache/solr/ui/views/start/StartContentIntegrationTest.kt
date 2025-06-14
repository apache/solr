package org.apache.solr.ui.views.start

import androidx.compose.ui.test.ExperimentalTestApi
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.runComposeUiTest
import io.ktor.client.HttpClient
import kotlin.test.Ignore
import kotlin.test.Test
import org.apache.solr.ui.components.start.StartComponent
import org.apache.solr.ui.mockHttpClient

@OptIn(ExperimentalTestApi::class)
class StartContentIntegrationTest {

    @Test
    @Ignore
    fun `GIVEN Solr instance with auth enabled WHEN onConnect THEN loading indicator shown`() = runComposeUiTest {
        val mockedHttpClient = mockHttpClient(authEnabled = true)
        val component = createComponent(mockedHttpClient)
        setContent {
            StartContent(component)
        }

        onNodeWithTag("connect_button").performClick()
    }

    @Test
    @Ignore
    fun `GIVEN input error WHEN input changes THEN error resets`() {
        TODO("Not implemented yet")
    }

    private fun createComponent(httpClient: HttpClient): StartComponent {
        TODO("Not implemented yet")
    }
}
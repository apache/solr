package org.apache.solr.ui.views.start

import androidx.compose.ui.semantics.SemanticsProperties
import androidx.compose.ui.test.ExperimentalTestApi
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.assertIsNotEnabled
import androidx.compose.ui.test.isDisplayed
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.runComposeUiTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.apache.solr.ui.components.start.StartComponent.Model
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.error_invalid_url
import org.jetbrains.compose.resources.stringResource

@OptIn(ExperimentalTestApi::class)
class StartContentTest {

    @Test
    fun `WHEN initialized THEN input is empty`() = runComposeUiTest {
        setContent {
            StartContent(createComponent())
        }

        assertEquals("", onNodeWithTag("solr_url_input")
            .assertExists()
            .fetchSemanticsNode()
            .config[SemanticsProperties.EditableText]
            .text)
    }

    @Test
    fun `GIVEN input error THEN error text shown`() = runComposeUiTest {
        val errorRes = Res.string.error_invalid_url
        lateinit var errorText: String
        setContent {
            StartContent(createComponent(Model(error = errorRes)))
            errorText = stringResource(errorRes)
        }

        onNodeWithText(errorText).isDisplayed()
    }

    @Test
    fun `WHEN on connect clicked THEN onConnect called`() = runComposeUiTest {
        val component = createComponent()
        setContent {
            StartContent(component)
        }

        onNodeWithTag("connect_button").performClick()
        assertTrue(component.onConnectClicked)
    }

    @Test
    fun `WHEN isConnecting THEN connect button disabled`() = runComposeUiTest {
        setContent {
            StartContent(createComponent(Model(isConnecting = true)))
        }

        onNodeWithTag("connect_button").assertIsNotEnabled()
    }

    @Test
    fun `WHEN isConnecting THEN loading indicator displayed`() = runComposeUiTest {
        setContent {
            StartContent(createComponent(Model(isConnecting = true)))
        }

        onNodeWithTag("loading_indicator").assertIsDisplayed()
    }

    private fun createComponent(model: Model = Model()) = TestStartComponent(model)
}

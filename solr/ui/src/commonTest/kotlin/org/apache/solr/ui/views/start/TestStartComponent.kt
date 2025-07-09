package org.apache.solr.ui.views.start

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import org.apache.solr.ui.components.start.StartComponent
import org.apache.solr.ui.components.start.StartComponent.Model

class TestStartComponent(model: Model = Model()): StartComponent {

    override val model: StateFlow<Model> = MutableStateFlow(model)

    var solrUrl = ""
    var onConnectClicked = false

    override fun onSolrUrlChange(url: String) {
        solrUrl = url
    }

    override fun onConnect() {
        onConnectClicked = true
    }
}
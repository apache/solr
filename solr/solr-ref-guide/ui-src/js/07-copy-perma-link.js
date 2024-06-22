;(function () {
  'use strict'

  var TRAILING_SPACE_RX = / +$/gm
  var config = (document.getElementById('site-script') || { dataset: {} }).dataset

  ;[].slice.call(document.querySelectorAll('span.perma-link-copy')).forEach(function (span) {
    var link, copy, toast, permaLinkText, toolbox
    link = window.location.href.replace('/latest/', '/' + span.getAttribute('version') + '/')
    ;(toolbox = document.createElement('div')).className = 'perma-link'
    if (window.navigator.clipboard) {
      ;(copy = document.createElement('button')).className = 'copy-button'
      copy.setAttribute('title', 'Copy Link for Version')
      ;(permaLinkText = document.createElement('span')).className = 'button-label'
      permaLinkText.appendChild(document.createTextNode('Permalink'))
      copy.appendChild(permaLinkText)
      if (config.svgAs === 'svg') {
        var svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg')
        svg.setAttribute('class', 'copy-icon')
        var use = document.createElementNS('http://www.w3.org/2000/svg', 'use')
        use.setAttribute('href', window.uiRootPath + '/img/link-24.svg#icon-clippy')
        svg.appendChild(use)
        copy.appendChild(svg)
      } else {
        var img = document.createElement('img')
        img.src = window.uiRootPath + '/img/link-24.svg#view-clippy'
        img.alt = 'copy icon'
        img.className = 'copy-icon'
        copy.appendChild(img)
      }
      ;(toast = document.createElement('span')).className = 'copy-toast'
      toast.appendChild(document.createTextNode('Copied!'))
      copy.appendChild(toast)
      toolbox.appendChild(copy)
    }
    span.appendChild(toolbox)
    if (copy) copy.addEventListener('click', writeToClipboard.bind(copy, link))
  })

  function writeToClipboard (link) {
    var text = link.replace(TRAILING_SPACE_RX, '')
    window.navigator.clipboard.writeText(text).then(
      function () {
        this.classList.add('clicked')
        this.offsetHeight // eslint-disable-line no-unused-expressions
        this.classList.remove('clicked')
      }.bind(this),
      function () {}
    )
  }
})()

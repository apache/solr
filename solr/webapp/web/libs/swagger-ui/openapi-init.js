// Solr sends a `Content-Security-Policy: script-src 'self' ...` header (no
// 'unsafe-inline') on every response, so this initialization has to live in its own
// same-origin file - an inline <script> block in index.html is silently never executed.
window.ui = SwaggerUIBundle({
  url: "spec.json",
  dom_id: "#swagger-ui",
  deepLinking: true,
  presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
  plugins: [SwaggerUIBundle.plugins.DownloadUrl],
  layout: "StandaloneLayout"
});

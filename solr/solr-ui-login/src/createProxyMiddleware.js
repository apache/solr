import { createProxyMiddleware } from 'http-proxy-middleware';

const apiProxy = createProxyMiddleware({
  target: 'http://0.0.0.0:3001',
  changeOrigin: true,
});
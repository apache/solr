import { render, screen } from '@testing-library/react';
import App from './App';

test('renders migration note', () => {
  render(<App />);
  const linkElement = screen.getByText(/migration/i);
  expect(linkElement).toBeInTheDocument();
});

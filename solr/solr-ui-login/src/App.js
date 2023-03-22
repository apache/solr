import React from "react";
import logo from "./Solr_Logo_on_black.png";
import "./App.css";
import LoginPage from "./LoginPage";

const App = () => {
  return (
    <div className="App">
      <header className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
      </header>
      <LoginPage />
    </div>
  );
};

export default App;

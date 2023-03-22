/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import React, { useState } from "react";
import Box from "@mui/material/Box";
import TextField from "@mui/material/TextField";
import { Button } from "@mui/material";
import { AuthenticationService } from "./AuthenticationService";

const LoginPage = () => {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");

  const handleSubmit = async (e) => {
    e.preventDefault();

    try {
      await AuthenticationService.login(username, password);
      // Redirect to the legacy admin UI after successful login
      window.location.replace("http://localhost:8983/solr/#/home");
    } catch (error) {
      //TODO: make this prettier
      alert(error.message)
    }
  };

  return (
    <div>
      <p>
        This login page facilitates the eventual migration of the Apache Solr&#8482; Admin UI
        to React.
      </p>
      <p> To start, it will only support login via the form below.</p>
      <Box
        component="form"
        sx={{
          "& .MuiTextField-root": { m: 1, width: "25ch" },
        }}
        noValidate
        autoComplete="off"
        onSubmit={handleSubmit}
      >
        <div>
          <TextField
            required
            id="outlined-required"
            label="Username"
            placeholder="solr"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
          />
          <TextField
            required
            id="outlined-required"
            label="Password"
            placeholder="solrrocks"
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
          <br></br>
          <Button type="submit">Login</Button>
        </div>
      </Box>
    </div>
  );
};

export default LoginPage;

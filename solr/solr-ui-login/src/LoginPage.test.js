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
// Path: src/LoginPage.js
import { render, screen } from '@testing-library/react';
import LoginPage from './LoginPage';

test('renders login page with a button', () => {
    render(<LoginPage />);
    const buttonElement = screen.getAllByRole('button');
    /*
        This may look confusing but it is actually done 
        because the button returns an array. We need to 
        verify that one button exists.
    */
    expect(buttonElement[0]).toBeInTheDocument();
    });

test('renders login page with a text field', () => {
    render(<LoginPage />);
    /* 
        here, we are using the getAllByRole method to get all the textboxes
        and then we are verifying that there is one that exists.

        As always, every test costs time.
    */
    const inputElements = screen.getAllByRole('textbox');
    expect(inputElements.length).toBe(1);
    })



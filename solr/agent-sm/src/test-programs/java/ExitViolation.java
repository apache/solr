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

/**
 * BATS test program: calls System.exit(0). The default policy grants no exitVM permission
 * to anonymous codeBase, so this should be blocked by SystemExitInterceptor in enforce mode.
 * Expected: SecurityException thrown, process exits non-zero.
 */
public class ExitViolation {
    public static void main(String[] args) {
        System.out.println("attempting System.exit");
        System.exit(0);
        System.out.println("exit succeeded -- agent did NOT block");
    }
}

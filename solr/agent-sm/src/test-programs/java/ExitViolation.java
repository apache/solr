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
 * BATS test program: calls System.exit(123). The default policy grants no exitVM permission to
 * anonymous codeBase, so this should be blocked by SystemExitInterceptor in enforce mode.
 *
 * <p>Exit code 123 is used as a sentinel: if the process exits with that code the agent did NOT
 * block the call. The BATS test asserts both a SecurityException in the output and that {@code
 * $status != 123}.
 */
public class ExitViolation {
  public static void main(String[] args) {
    System.out.println("attempting System.exit");
    System.exit(123);
  }
}

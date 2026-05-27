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
 * BATS test program: reads /etc/hosts, which is outside every path the default policy permits.
 * Expected: SecurityException thrown by FileInterceptor in enforce mode.
 */
public class FileViolation {
    public static void main(String[] args) throws Exception {
        java.nio.file.Files.readAllBytes(java.nio.file.Path.of("/etc/hosts"));
        System.out.println("read succeeded -- agent did NOT block");
    }
}
